package engine

import (
	"fmt"

	commonrank "common/rank"
	mongoTask "common/task/mongo"
	mongodbmodule "golib/mongodb"
	"golib/queue"
	"golib/zaplog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DAO 封装排行榜业务层的 MongoDB 持久化操作。
// 所有写入、更新、删除操作（包括配置类）均通过 mongoTask.GWriteTaskBuilder 异步队列执行，不阻塞业务 goroutine。
// 批量删除（DeleteAllByBizId）因需要 DeleteMany 语义，暂通过 session 同步执行（队列目前仅支持 DeleteOne）。
// 读操作保持同步。队列生命周期由外部通过 queue.InitQueues() / queue.Shutdown() 管理。
type DAO struct {
	dbName string
}

func NewDAO(dbName string) *DAO {
	if dbName == "" {
		return nil
	}
	return &DAO{dbName: dbName}
}

func (d *DAO) available() bool { return d != nil && d.dbName != "" }

func (d *DAO) session() *mongodbmodule.Session {
	return mongodbmodule.Main.TakeSession()
}

// --- 活动注册表 ---

// PeriodicSavedState 周期排行榜运行时状态，与逻辑活动的 RankConfigDoc 一同持久化。
type PeriodicSavedState struct {
	CycleDays      int32 `bson:"cycleDays"`
	TotalOpenTime  int64 `bson:"totalOpenTime"`
	TotalCloseTime int64 `bson:"totalCloseTime"`
	CurrentRound   int32 `bson:"currentRound"`
	RoundOpenTime  int64 `bson:"roundOpenTime"`
	RoundCloseTime int64 `bson:"roundCloseTime"`
}

// RankConfigDoc 排行榜配置文档，所有业务类型统一存储于 commonrank.CT_RANK_CONFIG 集合。
type RankConfigDoc struct {
	BizKey   string              `bson:"_id"`    // "{bizType}:{actID}" 格式，跨业务唯一
	Config   Config              `bson:"config"`
	Periodic *PeriodicSavedState `bson:"periodic,omitempty"` // 非 nil 表示周期排行榜
}

func (d *DAO) SaveRankConfig(bizKey string, cfg Config) error {
	if !d.available() {
		return nil
	}
	persisted := cfg
	persisted.RobotTiers = nil
	persisted.RobotInfos = nil
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_CONFIG,
		bizKey,
		bson.M{"_id": bizKey},
		bson.M{"$set": bson.M{"config": persisted}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save rank config bizKey=%s: %v", bizKey, err)
	}
	return nil
}

// SavePeriodicState 更新逻辑活动配置文档中的周期运行时状态字段。
func (d *DAO) SavePeriodicState(bizKey string, state PeriodicSavedState) error {
	if !d.available() {
		return nil
	}
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_CONFIG,
		bizKey,
		bson.M{"_id": bizKey},
		bson.M{"$set": bson.M{"periodic": state}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save periodic state bizKey=%s: %v", bizKey, err)
	}
	return nil
}

// SaveRankConfigWithPeriodic 原子保存排行榜配置和周期状态（首次注册周期活动时使用）。
func (d *DAO) SaveRankConfigWithPeriodic(bizKey string, cfg Config, state PeriodicSavedState) error {
	if !d.available() {
		return nil
	}
	persisted := cfg
	persisted.RobotTiers = nil
	persisted.RobotInfos = nil
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_CONFIG,
		bizKey,
		bson.M{"_id": bizKey},
		bson.M{"$set": bson.M{"config": persisted, "periodic": state}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save rank config with periodic bizKey=%s: %v", bizKey, err)
	}
	return nil
}

func (d *DAO) LoadAllRankConfigs() ([]RankConfigDoc, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, commonrank.CT_RANK_CONFIG, bson.M{})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []RankConfigDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (d *DAO) DeleteRankConfig(bizKey string) error {
	if !d.available() {
		return nil
	}
	task := mongoTask.GWriteTaskBuilder.BuildDeleteTask(
		commonrank.CT_RANK_CONFIG,
		bizKey,
		bson.M{"_id": bizKey},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: delete rank config bizKey=%s: %v", bizKey, err)
	}
	return nil
}

// --- 分组数据 ---

// GroupDoc 分组文档。
type GroupDoc struct {
	ID      string `bson:"_id"` // "{bizId}:{groupID}"
	BizId   string `bson:"bizId"`
	GroupID int32  `bson:"groupId"`
	Group   Group  `bson:"group"`
}

func groupDocID(bizId string, groupID int32) string {
	return fmt.Sprintf("%s:%d", bizId, groupID)
}

func (d *DAO) SaveGroup(bizId string, group *Group) error {
	if !d.available() {
		return nil
	}
	g := *group // copy to avoid aliasing
	docID := groupDocID(bizId, g.GroupID)
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_GROUP,
		docID,
		bson.M{"_id": docID},
		bson.M{"$set": bson.M{"bizId": bizId, "groupId": g.GroupID, "group": g}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save group bizId=%s groupId=%d: %v", bizId, g.GroupID, err)
	}
	return nil
}

func (d *DAO) LoadGroups(bizId string) ([]*Group, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, commonrank.CT_RANK_GROUP, bson.M{"bizId": bizId})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []GroupDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	groups := make([]*Group, 0, len(docs))
	for _, doc := range docs {
		g := doc.Group
		groups = append(groups, &g)
	}
	return groups, nil
}

// --- 成员映射 ---

// MemberDoc 成员→分组映射文档。
type MemberDoc struct {
	ID      string `bson:"_id"` // "{bizId}:{userID}"
	BizId   string `bson:"bizId"`
	UserID  int64  `bson:"userId"`
	GroupID int32  `bson:"groupId"`
}

func memberDocID(bizId string, userID int64) string {
	return fmt.Sprintf("%s:%d", bizId, userID)
}

func (d *DAO) SaveMember(bizId string, userID int64, groupID int32) error {
	if !d.available() {
		return nil
	}
	docID := memberDocID(bizId, userID)
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_MEMBER,
		docID,
		bson.M{"_id": docID},
		bson.M{"$set": bson.M{"bizId": bizId, "userId": userID, "groupId": groupID}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save member bizId=%s user=%d: %v", bizId, userID, err)
	}
	return nil
}

func (d *DAO) GetMember(bizId string, userID int64) (int32, bool, error) {
	if !d.available() {
		return 0, false, nil
	}
	var doc MemberDoc
	err := d.session().FindOne(d.dbName, commonrank.CT_RANK_MEMBER,
		bson.M{"_id": memberDocID(bizId, userID)}, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, false, nil
		}
		return 0, false, err
	}
	return doc.GroupID, true, nil
}

func (d *DAO) LoadAllMembers(bizId string) (map[int64]int32, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, commonrank.CT_RANK_MEMBER, bson.M{"bizId": bizId})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []MemberDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	result := make(map[int64]int32, len(docs))
	for _, doc := range docs {
		result[doc.UserID] = doc.GroupID
	}
	return result, nil
}

// --- 机器人状态 ---

// RobotDoc 机器人状态文档。
type RobotDoc struct {
	ID       string     `bson:"_id"` // "{bizId}:{groupID}:{memberID}"
	BizId    string     `bson:"bizId"`
	GroupID  int32      `bson:"groupId"`
	MemberID int64      `bson:"memberId"`
	State    robotState `bson:"state"`
}

func robotDocID(bizId string, groupID int32, memberID int64) string {
	return fmt.Sprintf("%s:%d:%d", bizId, groupID, memberID)
}

func (d *DAO) SaveRobots(bizId string, groupID int32, robots []*robotState) error {
	if !d.available() || len(robots) == 0 {
		return nil
	}
	for _, r := range robots {
		rCopy := *r
		docID := robotDocID(bizId, groupID, rCopy.MemberID)
		task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
			commonrank.CT_RANK_ROBOT,
			docID,
			bson.M{"_id": docID},
			bson.M{"$set": bson.M{"bizId": bizId, "groupId": groupID, "memberId": rCopy.MemberID, "state": rCopy}},
		)
		if err := queue.PushMongoTask(task); err != nil {
			zaplog.LoggerSugar.Errorf("rank engine dao: save robot bizId=%s group=%d member=%d: %v", bizId, groupID, rCopy.MemberID, err)
		}
	}
	return nil
}

func (d *DAO) LoadRobots(bizId string, groupID int32) ([]*robotState, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, commonrank.CT_RANK_ROBOT,
		bson.M{"bizId": bizId, "groupId": groupID})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []RobotDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	robots := make([]*robotState, 0, len(docs))
	for _, doc := range docs {
		r := doc.State
		robots = append(robots, &r)
	}
	return robots, nil
}

// EnsureIndexes 创建 MongoDB 索引，并清理历史遗留的错误索引。
func (d *DAO) EnsureIndexes() {
	if !d.available() {
		return
	}
	session := d.session()
	ctx, cancel := session.GetDefaultContext()
	defer cancel()

	// 清理旧版本遗留的错误唯一索引（这些集合文档无 userId 字段，唯一索引导致第二条插入失败）。
	for _, coll := range []string{commonrank.CT_RANK_GROUP, commonrank.CT_RANK_INST} {
		_, _ = session.Database(d.dbName).Collection(coll).Indexes().DropOne(ctx, "idx_userId_unique")
	}

	indexes := map[string][]mongo.IndexModel{
		commonrank.CT_RANK_GROUP: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
		},
		commonrank.CT_RANK_MEMBER: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
			{Keys: bson.D{{Key: "userId", Value: 1}}, Options: options.Index().SetName("idx_userId")},
		},
		commonrank.CT_RANK_ROBOT: {
			{Keys: bson.D{{Key: "bizId", Value: 1}, {Key: "groupId", Value: 1}}, Options: options.Index().SetName("idx_bizId_groupId")},
		},
		commonrank.CT_RANK_CLAIM: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
			{Keys: bson.D{{Key: "userId", Value: 1}}, Options: options.Index().SetName("idx_userId")},
		},
		commonrank.CT_RANK_SCORE: {
			{Keys: bson.D{{Key: "bizId", Value: 1}, {Key: "groupId", Value: 1}}, Options: options.Index().SetName("idx_bizId_groupId")},
		},
		commonrank.CT_RANK_SETTLED: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
		},
		commonrank.CT_RANK_INST: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
		},
	}
	for coll, idxs := range indexes {
		if _, err := session.Database(d.dbName).Collection(coll).Indexes().CreateMany(ctx, idxs); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: create indexes for %s: %v", coll, err)
		}
	}
}

// --- 奖励领取记录 ---

type ClaimDoc struct {
	ID        string `bson:"_id"`
	BizId     string `bson:"bizId"`
	UserID    int64  `bson:"userId"`
	ClaimTime int64  `bson:"claimTime"`
}

func claimDocID(bizId string, userID int64) string {
	return fmt.Sprintf("%s:%d", bizId, userID)
}

// SaveClaimIfNotExists atomically creates the claim record only if it doesn't already exist.
// Uses $setOnInsert so that an existing document is never modified.
// Returns isFirst=true when this call inserted the document (first claim).
// Returns isFirst=false with the existing claimTime when it already existed.
func (d *DAO) SaveClaimIfNotExists(bizId string, userID int64, claimTime int64) (isFirst bool, existingTime int64, err error) {
	if !d.available() {
		return true, claimTime, nil
	}
	docID := claimDocID(bizId, userID)
	result, upsertErr := d.session().UpsertOne(d.dbName, commonrank.CT_RANK_CLAIM,
		bson.M{"_id": docID},
		bson.M{"$setOnInsert": bson.M{
			"_id":       docID,
			"bizId":     bizId,
			"userId":    userID,
			"claimTime": claimTime,
		}},
	)
	if upsertErr != nil {
		return false, 0, upsertErr
	}
	if result.UpsertedCount > 0 {
		return true, claimTime, nil
	}
	// Document already existed — read the recorded claim time.
	ct, found, readErr := d.GetClaim(bizId, userID)
	if readErr != nil {
		return false, 0, readErr
	}
	if !found {
		// Unlikely but safe: treat as first claim.
		return true, claimTime, nil
	}
	return false, ct, nil
}

func (d *DAO) SaveClaim(bizId string, userID int64, claimTime int64) error {
	if !d.available() {
		return nil
	}
	docID := claimDocID(bizId, userID)
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_CLAIM,
		docID,
		bson.M{"_id": docID},
		bson.M{"$set": bson.M{"bizId": bizId, "userId": userID, "claimTime": claimTime}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save claim bizId=%s user=%d: %v", bizId, userID, err)
	}
	return nil
}

func (d *DAO) GetClaim(bizId string, userID int64) (int64, bool, error) {
	if !d.available() {
		return 0, false, nil
	}
	var doc ClaimDoc
	err := d.session().FindOne(d.dbName, commonrank.CT_RANK_CLAIM,
		bson.M{"_id": claimDocID(bizId, userID)}, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, false, nil
		}
		return 0, false, err
	}
	return doc.ClaimTime, true, nil
}

// --- 成员积分持久化 ---

// ScoreDoc 成员积分文档（rank:mb 持久化备份）。
type ScoreDoc struct {
	ID         string                 `bson:"_id"` // "{bizId}:{groupId}:{userID}"
	BizId      string                 `bson:"bizId"`
	GroupID    int32                  `bson:"groupId"`
	UserID     int64                  `bson:"userId"`
	Score      int64                  `bson:"score"`
	EnterTime  int64                  `bson:"enterTime"`
	UpdateTime int64                  `bson:"updateTime"`
	Sequence   int64                  `bson:"sequence"`
	AvatarInfo *commonrank.AvatarInfo `bson:"avatarInfo,omitempty"`
}

func scoreDocID(bizId string, groupID int32, userID int64) string {
	return fmt.Sprintf("%s:%d:%d", bizId, groupID, userID)
}

// SaveScore 以异步 write-through 方式持久化成员积分（不阻塞调用方）。
// 使用 $setOnInsert 保证 enterTime 和 sequence 仅在首次写入时设置，后续更新不覆盖。
func (d *DAO) SaveScore(bizId string, groupID int32, userID int64, score int64, enterTime int64, sequence int64, updateTime int64, avatarInfo *commonrank.AvatarInfo) error {
	if !d.available() {
		return nil
	}
	var aiCopy *commonrank.AvatarInfo
	if avatarInfo != nil {
		tmp := *avatarInfo
		aiCopy = &tmp
	}
	docID := scoreDocID(bizId, groupID, userID)
	setFields := bson.M{
		"score":      score,
		"updateTime": updateTime,
		"groupId":    groupID,
		"bizId":      bizId,
		"userId":     userID,
	}
	if aiCopy != nil {
		setFields["avatarInfo"] = aiCopy
	}
	setOnInsert := bson.M{"enterTime": enterTime}
	if sequence > 0 {
		setOnInsert["sequence"] = sequence
	}
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_SCORE,
		docID,
		bson.M{"_id": docID},
		bson.M{
			"$set":         setFields,
			"$setOnInsert": setOnInsert,
		},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save score bizId=%s group=%d user=%d: %v", bizId, groupID, userID, err)
	}
	return nil
}

func (d *DAO) LoadGroupScores(bizId string, groupID int32) ([]ScoreDoc, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, commonrank.CT_RANK_SCORE,
		bson.M{"bizId": bizId, "groupId": groupID})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []ScoreDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// --- 结算快照持久化 ---

// SettledDoc 结算快照文档（rank:settled 持久化备份）。
type SettledDoc struct {
	ID         string                          `bson:"_id"` // "{bizId}:{groupId}"
	BizId      string                          `bson:"bizId"`
	GroupID    int32                           `bson:"groupId"`
	Snapshots  []commonrank.RankMemberSnapshot `bson:"snapshots"`
	SettleTime int64                           `bson:"settleTime"`
}

func settledDocID(bizId string, groupID int32) string {
	return fmt.Sprintf("%s:%d", bizId, groupID)
}

func (d *DAO) SaveSettled(bizId string, groupID int32, snaps []commonrank.RankMemberSnapshot, settleTime int64) error {
	if !d.available() {
		return nil
	}
	snapsCopy := make([]commonrank.RankMemberSnapshot, len(snaps))
	copy(snapsCopy, snaps)
	docID := settledDocID(bizId, groupID)
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_SETTLED,
		docID,
		bson.M{"_id": docID},
		bson.M{"$set": bson.M{"bizId": bizId, "groupId": groupID, "snapshots": snapsCopy, "settleTime": settleTime}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save settled bizId=%s group=%d: %v", bizId, groupID, err)
	}
	return nil
}

func (d *DAO) LoadGroupSettled(bizId string, groupID int32) ([]commonrank.RankMemberSnapshot, error) {
	if !d.available() {
		return nil, nil
	}
	var doc SettledDoc
	err := d.session().FindOne(d.dbName, commonrank.CT_RANK_SETTLED,
		bson.M{"_id": settledDocID(bizId, groupID)}, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return doc.Snapshots, nil
}

// --- 榜单实例元数据 ---

// RankInstDoc 榜单实例文档，对应 Redis 中的 rank:inst:{instanceId}。
type RankInstDoc struct {
	ID         string                  `bson:"_id"` // "{bizId}:{groupID}"
	BizId      string                  `bson:"bizId"`
	GroupID    int32                   `bson:"groupId"`
	InstanceID string                  `bson:"instanceId"`
	Instance   commonrank.RankInstance `bson:"instance"`
}

func rankInstDocID(bizId string, groupID int32) string {
	return fmt.Sprintf("%s:%d", bizId, groupID)
}

// SaveRankInst 异步持久化榜单实例元数据到 MongoDB。
func (d *DAO) SaveRankInst(bizId string, groupID int32, inst commonrank.RankInstance) error {
	if !d.available() {
		return nil
	}
	docID := rankInstDocID(bizId, groupID)
	task := mongoTask.GWriteTaskBuilder.BuildUpsertTask(
		commonrank.CT_RANK_INST,
		docID,
		bson.M{"_id": docID},
		bson.M{"$set": bson.M{"bizId": bizId, "groupId": groupID, "instanceId": inst.InstanceId, "instance": inst}},
	)
	if err := queue.PushMongoTask(task); err != nil {
		zaplog.LoggerSugar.Errorf("rank engine dao: save rank inst bizId=%s group=%d: %v", bizId, groupID, err)
	}
	return nil
}

// LoadGroupInst 同步读取指定分组的榜单实例元数据。未找到时返回 nil, nil。
func (d *DAO) LoadGroupInst(bizId string, groupID int32) (*commonrank.RankInstance, error) {
	if !d.available() {
		return nil, nil
	}
	var doc RankInstDoc
	err := d.session().FindOne(d.dbName, commonrank.CT_RANK_INST,
		bson.M{"_id": rankInstDocID(bizId, groupID)}, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	inst := doc.Instance
	return &inst, nil
}

// DeleteAllByBizId 同步删除指定 bizId 下的全部关联文档（DeleteMany 语义）。
func (d *DAO) DeleteAllByBizId(bizId string) error {
	if !d.available() {
		return nil
	}
	filter := bson.M{"bizId": bizId}
	colls := []string{
		commonrank.CT_RANK_GROUP,
		commonrank.CT_RANK_MEMBER,
		commonrank.CT_RANK_ROBOT,
		commonrank.CT_RANK_CLAIM,
		commonrank.CT_RANK_SCORE,
		commonrank.CT_RANK_SETTLED,
		commonrank.CT_RANK_INST,
	}
	session := d.session()
	for _, coll := range colls {
		if _, err := session.DeleteMany(d.dbName, coll, filter); err != nil {
			zaplog.LoggerSugar.Errorf("rank engine dao: DeleteAllByBizId %s bizId=%s: %v", coll, bizId, err)
		}
	}
	return nil
}

// QueueDeleteDocIDs 通过异步队列为已知 docID 列表逐条推送 DeleteOne 任务。
func (d *DAO) QueueDeleteDocIDs(coll string, docIDs []string) {
	if !d.available() || len(docIDs) == 0 {
		return
	}
	for _, docID := range docIDs {
		id := docID
		task := mongoTask.GWriteTaskBuilder.BuildDeleteTask(coll, id, bson.M{"_id": id})
		if err := queue.PushMongoTask(task); err != nil {
			zaplog.LoggerSugar.Errorf("rank engine dao: QueueDeleteDocIDs %s id=%s: %v", coll, id, err)
		}
	}
}
