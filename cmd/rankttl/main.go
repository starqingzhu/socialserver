// Command rankttl 是一次性运维工具：给线上遗留的「永久 rank key」补上有界 TTL。
//
// 为什么需要它：升级前的一次性活动走完 Tick→Settle 之后，它的 11 个 rank key 全部没有 TTL，
// 永久残留在 Redis 里（docs/rank_optimization.md 缺陷 6）。新代码只在「该活动仍在内存中」时
// 才有机会补 TTL，而 Manager.syncFromMongo 与 registerFromBizId 都会跳过关闭超过 7 天的活动
// （`now > CloseTime + 7*86400000`），因此**存量冷活动的永久 key 永远不会自动收敛**。
// 新增的与进行中的活动不受影响：它们在启动注册、下一次写入、或重启后的首次 Tick 时会自行补上。
//
// 安全性由一条规则保证：只处理 PTTL == -1（永不过期）的 key。已经带 TTL 的 key 一律不碰，
// 所以本工具在结构上不可能缩短任何既有 TTL。活跃活动的 key 要么已带 TTL，要么下一次写入会按
// 绝对过期时刻重设，两种情况下本工具的结果都会被后续写入覆盖，不会造成提前删除。
//
// 用法（可执行文件必须与 .devops.yaml 同目录，与 socialserver 一致——yamlcfg.LoadYamlCfg
// 是按可执行文件所在目录找配置的；这样也不必把 Redis 密码写进命令行）：
//
//	cd socialserver && go build -o ../bin/rankttl ./cmd/rankttl
//	cd ../bin && ./rankttl           # dry-run：只报告将要改动的 key 数，不写入
//	cd ../bin && ./rankttl -apply    # 实际写入
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/yamlcfg"
)

// PTTL 的两个负值哨兵（go-redis 原样透传 Redis 语义）。
const (
	pttlNoExpire = -1 * time.Nanosecond // key 存在但永不过期
	pttlMissing  = -2 * time.Nanosecond // key 已不存在（SCAN 之后被删）
)

// ttlClass 描述一类 rank key 及其应补的 TTL。
type ttlClass struct {
	// name 是报告里显示的分类名。
	name string
	// prefix 是前缀匹配串（含末尾冒号）；exact 非空时忽略本字段。
	prefix string
	// exact 是精确匹配的完整 key，用于名字里没有冒号后缀的 rank:{active_services}。
	exact string
	// ttl 是要补的 TTL；skip 为 true 时无意义。
	ttl time.Duration
	// skip 表示「认识但绝不改动」。锁 key 属于这类：它们自带短 TTL，且可能正被
	// 运行中的活动持有。
	skip bool
}

// classes 是分类表的唯一来源。
//
// 前缀全部取自 common/redis 的导出常量而不是字面量。这样新增一类 rank key 时，未登记的 key 会
// 落进「未识别」桶并被打印出来，而不是被静默跳过或被错误归类——工具宁可少做也不猜。
//
// 两个易错点，HasPrefix 恰好都不会串，但改动本表前必须重新核对：
//   - rank:settled:（数据）与 rank:settle:（锁）只差一个字符；
//   - rank:robots: / rank:robot_infos:（数据）与 rank:robot_tick:（锁）同理。
//
// 锁 key 必须显式登记为 skip，否则一旦它们意外失去 TTL（例如某个 SET NX EX 写漏了 EX），
// 本工具会把它们当成数据 key 加上 14 天，反而制造出长期存活的锁。
var classes = []ttlClass{
	// 「无天然结束时刻的冷数据」：与各自的写入方保持一致，都用 ColdDataTTL。
	{name: "rank:def", prefix: rediskeys.RankDefKeyPrefix + ":", ttl: commonrank.ColdDataTTL},
	{name: "rank:member_index", prefix: rediskeys.RankMemberIndexKeyPrefix + ":", ttl: commonrank.ColdDataTTL},
	{name: "rank:{active_services}", exact: rediskeys.RankActiveServicesKey, ttl: commonrank.ColdDataTTL},

	// 活动数据：与 Store.ExpireLiveData 的 key 集合逐项对齐，TTL 为 SettledCacheTTL。
	{name: "rank:inst", prefix: rediskeys.RankInstKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:mb", prefix: rediskeys.RankMbKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:seq", prefix: rediskeys.RankSeqKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:settled", prefix: rediskeys.RankSettledKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:meta", prefix: rediskeys.RankMetaKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:groups", prefix: rediskeys.RankGroupsKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:members", prefix: rediskeys.RankMembersKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:robots", prefix: rediskeys.RankRobotsKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:robot_infos", prefix: rediskeys.RankRobotInfosKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:claims", prefix: rediskeys.RankClaimsKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},
	{name: "rank:mongo_chk", prefix: rediskeys.RankMongoCheckedKeyPrefix + ":", ttl: commonrank.SettledCacheTTL},

	// 锁 key：自带短 TTL，绝不改动。
	{name: "rank:settle(锁)", prefix: "rank:settle:", skip: true},
	{name: "rank:robot_tick(锁)", prefix: "rank:robot_tick:", skip: true},
}

// classify 把 key 归入 classes 中的某一类。
//
// 顺序敏感：rank:settle: 必须先于 rank:settled: 判定是不可能的（两者互不为前缀，见 classes
// 注释），但为了让「跳过」桶一定赢过「改 TTL」桶，这里按 skip 优先做两轮匹配——万一将来有人
// 往表里加了一个既是锁又和数据 key 同前缀的项，也不会被静默改掉。
func classify(key string) (ttlClass, bool) {
	for _, c := range classes {
		if !c.skip {
			continue
		}
		if c.exact != "" {
			if key == c.exact {
				return c, true
			}
			continue
		}
		if len(key) > len(c.prefix) && key[:len(c.prefix)] == c.prefix {
			return c, true
		}
	}
	for _, c := range classes {
		if c.skip {
			continue
		}
		if c.exact != "" {
			if key == c.exact {
				return c, true
			}
			continue
		}
		if len(key) > len(c.prefix) && key[:len(c.prefix)] == c.prefix {
			return c, true
		}
	}
	return ttlClass{}, false
}

// classStat 是单个分类的统计。
type classStat struct {
	name    string
	ttl     time.Duration
	skip    bool
	matched int // 匹配到的 key 数
	noTTL   int // 其中永不过期的
	updated int // 实际补上 TTL 的（dry-run 下为 0）
	failed  int
}

func main() {
	apply := flag.Bool("apply", false, "实际写入 TTL。默认 false，即 dry-run 只报告")
	pattern := flag.String("match", "rank:*", "SCAN 匹配模式")
	count := flag.Int64("count", 500, "SCAN 每次迭代返回的 key 数")
	flag.Parse()

	r, err := newRedisFromYaml()
	if err != nil {
		die(err)
	}
	defer func() { _ = r.Close() }()

	ctx := context.Background()
	keys, err := r.ScanAll(ctx, *pattern, *count)
	if err != nil {
		die(fmt.Errorf("SCAN %s: %w", *pattern, err))
	}

	stats := make(map[string]*classStat, len(classes))
	for _, c := range classes {
		stats[c.name] = &classStat{name: c.name, ttl: c.ttl, skip: c.skip}
	}
	unknown := make(map[string]int) // 未识别的 key 前缀 -> 计数
	live := 0                       // 已带 TTL、未改动的 key 数

	for _, key := range keys {
		c, ok := classify(key)
		if !ok {
			unknown[unknownPrefix(key)]++
			continue
		}
		st := stats[c.name]
		st.matched++
		if c.skip {
			continue
		}

		pttl, err := r.PTTL(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[warn] PTTL %s: %v\n", key, err)
			st.failed++
			continue
		}
		switch {
		case pttl == pttlMissing:
			// SCAN 与 PTTL 之间消失了：正常并发，不是问题。
			st.matched--
		case pttl >= 0 || pttl < pttlNoExpire:
			// 已带 TTL（或返回了预期外的负值，不猜、不改）。
			live++
		default:
			// pttl == pttlNoExpire：这正是本工具要修的那一类。
			st.noTTL++
			if !*apply {
				continue
			}
			if _, err := r.Expire(key, c.ttl); err != nil {
				fmt.Fprintf(os.Stderr, "[warn] EXPIRE %s %v: %v\n", key, c.ttl, err)
				st.failed++
				continue
			}
			st.updated++
		}
	}

	report(stats, unknown, live, len(keys), *apply)
	if !*apply {
		fmt.Println("\n这是 dry-run，没有任何写入。确认无误后加 -apply 再跑一次。")
	}
}

// unknownPrefix 把未识别的 key 归到一个可读的桶名上，避免打印成千上万条 key。
// rank key 的形态是 "rank:<段>..." 或 "rank:{<段>}..."，取到第二个分隔符为止。
func unknownPrefix(key string) string {
	rest := key[len("rank:"):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' || rest[i] == '}' {
			return "rank:" + rest[:i+1]
		}
	}
	return key
}

func report(stats map[string]*classStat, unknown map[string]int, live, scanned int, apply bool) {
	names := make([]string, 0, len(stats))
	for name := range stats {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Printf("扫描到 %d 个 key；其中已带 TTL、无需改动的 %d 个。\n\n", scanned, live)
	fmt.Printf("%-24s %-10s %8s %8s %8s\n", "分类", "补 TTL", "匹配", "无 TTL", "已写入")
	var totNoTTL, totUpdated, totSkip int
	for _, name := range names {
		st := stats[name]
		if st.matched == 0 {
			continue
		}
		if st.skip {
			fmt.Printf("%-24s %-10s %8d %8s %8s\n", st.name, "—(锁)", st.matched, "—", "—")
			totSkip += st.matched
			continue
		}
		ttlCol := st.ttl.String()
		if !apply {
			ttlCol = st.ttl.String() + "*"
		}
		fmt.Printf("%-24s %-10s %8d %8d %8d\n", st.name, ttlCol, st.matched, st.noTTL, st.updated)
		totNoTTL += st.noTTL
		totUpdated += st.updated
		if st.failed > 0 {
			fmt.Printf("  ^ 其中 %d 个写入失败\n", st.failed)
		}
	}
	if !apply {
		fmt.Println("\n* 列是 dry-run 下「将会」补上的 TTL，尚未写入。")
	}
	fmt.Printf("\n合计：待补 TTL %d 个", totNoTTL)
	if apply {
		fmt.Printf("，已写入 %d 个", totUpdated)
	}
	fmt.Printf("；跳过锁 key %d 个。\n", totSkip)

	if len(unknown) > 0 {
		fmt.Printf("\n未识别的 key 前缀（未做任何改动，请人工确认后再决定是否补进 cmd/rankttl/main.go 的 classes 表）：\n")
		keys := make([]string, 0, len(unknown))
		for k := range unknown {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %-32s %d 个\n", k, unknown[k])
		}
	}
}

// newRedisFromYaml 按 .devops.yaml 的 redis 段建连接。
// 解析规则与 internal.Server.loadRedisConfig 保持一致：host[:port]，密码与 DB 序号取第一条。
// 这里刻意复刻而不是调用它——那需要导入整个 socialserver/internal，为一个一次性工具不值得。
func newRedisFromYaml() (*goredis.Redis, error) {
	cfg, ok := yamlcfg.LoadYamlCfg()
	if !ok {
		return nil, fmt.Errorf("加载 .devops.yaml 失败：请确认可执行文件与 .devops.yaml 同目录")
	}
	if len(cfg.Redis) == 0 {
		return nil, fmt.Errorf(".devops.yaml 里没有 redis 配置")
	}

	out := &goredis.RedisConfig{}
	for _, item := range cfg.Redis {
		if item.Host == "" {
			continue
		}
		host := item.Host
		if item.Port > 0 {
			host = fmt.Sprintf("%s:%d", item.Host, item.Port)
		}
		if len(out.RedisAddrs) == 0 {
			out.RedisPasswd = item.Password
			out.RedisDBIndex = item.Index
		}
		out.RedisAddrs = append(out.RedisAddrs, host)
	}
	if len(out.RedisAddrs) == 0 {
		return nil, fmt.Errorf(".devops.yaml 的 redis 段没有有效 host")
	}

	// 只打印地址与 DB，不打印密码。
	fmt.Printf("Redis: addrs=%v db=%d\n", out.RedisAddrs, out.RedisDBIndex)
	r := goredis.NewRedis(out)
	if r == nil {
		return nil, fmt.Errorf("建 Redis 连接失败: %v", out.RedisAddrs)
	}
	return r, nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "rankttl:", err)
	os.Exit(1)
}
