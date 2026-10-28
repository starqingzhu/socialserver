package rankservice

import (
	"testing"
)

func TestParseRegistryMember(t *testing.T) {
	bizId, deadline, ok := parseRegistryMember("balloon_1001:1758000000000")
	if !ok || bizId != "balloon_1001" || deadline != 1758000000000 {
		t.Fatalf("unexpected parse result: bizId=%q deadline=%d ok=%v", bizId, deadline, ok)
	}
}

func TestParseRegistryMemberPeriodicRoundBizId(t *testing.T) {
	// 周期子轮次 bizId 形如 "camper_1001_r2"，本身不含冒号，取最后一个冒号切分仍然安全。
	bizId, deadline, ok := parseRegistryMember("camper_1001_r2:1758000000000")
	if !ok || bizId != "camper_1001_r2" || deadline != 1758000000000 {
		t.Fatalf("unexpected parse result: bizId=%q deadline=%d ok=%v", bizId, deadline, ok)
	}
}

func TestParseRegistryMemberInvalid(t *testing.T) {
	cases := []string{
		"",
		"noColonHere",
		"balloon_1001:",
		":1758000000000",
		"balloon_1001:notANumber",
	}
	for _, c := range cases {
		if _, _, ok := parseRegistryMember(c); ok {
			t.Fatalf("expected parse failure for %q", c)
		}
	}
}

