package game

import "time"

// RulesVersion is the popularity rules version used by new seasons.
// Every PopularityEvent stores its own version, so changing the rules never
// rewrites history (design §8).
const RulesVersion = 1

// NoveltyBP computes location novelty in basis points. A destination that has
// never been visited gets the full value; after a visit, the value climbs
// linearly back to full over 7 days, with a floor of 2,000.
func NoveltyBP(lastVisit *time.Time, at time.Time) int32 {
	if lastVisit == nil {
		return 10000
	}
	hours := at.Sub(*lastVisit).Hours()
	return clampBP(int64(hours*10000/(7*24)), 2000, 10000)
}

// FrequencyPenaltyBP computes the engagement multiplier from the number of posts
// that went live in the previous 24 hours: each post takes off 1,000, down to 5,000.
func FrequencyPenaltyBP(recentPosts int) int32 {
	return clampBP(10000-int64(recentPosts)*1000, 5000, 10000)
}

// Convert turns one bucket's reactions into extra followers and season score,
// using integer arithmetic so the result is deterministic:
// every like adds 0.3 followers and every comment adds 1.5 followers;
// season score = likes + 5 × comments.
func Convert(likes, comments int32) (deltaFollowers, deltaScore int64) {
	deltaFollowers = (int64(likes)*3000 + int64(comments)*15000) / 10000
	deltaScore = int64(likes) + 5*int64(comments)
	return deltaFollowers, deltaScore
}

func clampBP(v, lo, hi int64) int32 {
	if v < lo {
		return int32(lo)
	}
	if v > hi {
		return int32(hi)
	}
	return int32(v)
}
