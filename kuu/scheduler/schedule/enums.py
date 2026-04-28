from enum import IntEnum


class Weekday(IntEnum):
	Monday = 0
	Tuesday = 1
	Wednesday = 2
	Thursday = 3
	Friday = 4
	Saturday = 5
	Sunday = 6


class Month(IntEnum):
	January = 1
	February = 2
	March = 3
	April = 4
	May = 5
	June = 6
	July = 7
	August = 8
	September = 9
	October = 10
	November = 11
	December = 12


Mon = Weekday.Monday
Tue = Weekday.Tuesday
Wed = Weekday.Wednesday
Thu = Weekday.Thursday
Fri = Weekday.Friday
Sat = Weekday.Saturday
Sun = Weekday.Sunday

Jan = Month.January
Feb = Month.February
Mar = Month.March
Apr = Month.April
May = Month.May
Jun = Month.June
Jul = Month.July
Aug = Month.August
Sep = Month.September
Oct = Month.October
Nov = Month.November
Dec = Month.December
