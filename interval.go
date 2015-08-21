package main

import "time"

type Interval string

const (
	Hourly  Interval = "hourly"
	Daily   Interval = "daily"
	Weekly  Interval = "weekly"
	Monthly Interval = "monthly"
)

var Intervals = [...]Interval{Hourly, Daily, Weekly, Monthly}

func (interval Interval) CalcIndex(now time.Time, snapshotTime time.Time) int {
	firstMonday := time.Date(1970, 1, 5, 0, 0, 0, 0, time.UTC)
	switch interval {
	case Hourly:
		now = now.Truncate(time.Hour)
		snapshotTime = snapshotTime.Truncate(time.Hour)
		return int(now.Sub(snapshotTime).Hours())
	case Daily:
		nowDays := int(now.Sub(firstMonday).Hours() / 24)
		snapshotDays := int(snapshotTime.Sub(firstMonday).Hours() / 24)
		return nowDays - snapshotDays
	case Weekly:
		nowWeeks := int(now.Sub(firstMonday).Hours() / 24 / 7)
		snapshotWeeks := int(snapshotTime.Sub(firstMonday).Hours() / 24 / 7)
		return nowWeeks - snapshotWeeks
	case Monthly:
		return int(now.Month()) - int(snapshotTime.Month()) + 12*(now.Year()-snapshotTime.Year())
	}
	return 0
}

func (interval Interval) GetMaxIndex(limits Limits) int {
	switch interval {
	case Hourly:
		return limits.Hourly
	case Daily:
		return limits.Daily
	case Weekly:
		return limits.Weekly
	case Monthly:
		return limits.Monthly
	}
	return 0
}
