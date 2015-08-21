package main

import "fmt"

type Limits struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
}

func (l Limits) String() string {
	return fmt.Sprintf("Hourly=%d, Daily=%d, Weekly=%d, Monthly=%d", l.Hourly, l.Daily, l.Weekly, l.Monthly)
}

func (limits Limits) Merge(newLimits ...OptionalLimits) Limits {
	for _, l := range newLimits {
		if l.Hourly != nil {
			limits.Hourly = *l.Hourly
		}
		if l.Daily != nil {
			limits.Daily = *l.Daily
		}
		if l.Weekly != nil {
			limits.Weekly = *l.Weekly
		}
		if l.Monthly != nil {
			limits.Monthly = *l.Monthly
		}
	}
	return limits
}
