package balloon

import "common/rank"

func cloneSnapshots(src []rank.RankMemberSnapshot) []rank.RankMemberSnapshot {
	if len(src) == 0 {
		return nil
	}
	dst := make([]rank.RankMemberSnapshot, len(src))
	for i, item := range src {
		dst[i] = item
		if len(item.Extra) > 0 {
			dst[i].Extra = make(map[string]int64, len(item.Extra))
			for k, v := range item.Extra {
				dst[i].Extra[k] = v
			}
		}
	}
	return dst
}

func sliceSnapshots(src []rank.RankMemberSnapshot, start int64, end int64) []rank.RankMemberSnapshot {
	if len(src) == 0 {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if end < start {
		return []rank.RankMemberSnapshot{}
	}
	if start >= int64(len(src)) {
		return []rank.RankMemberSnapshot{}
	}
	if end >= int64(len(src)) {
		end = int64(len(src)) - 1
	}
	return cloneSnapshots(src[start : end+1])
}
