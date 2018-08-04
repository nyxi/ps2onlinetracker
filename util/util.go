package util

func IsIntInList(i int, list []int) bool {
	for c := 0; c < len(list); c++ {
		if i == list[c] {
			return true
		}
	}
	return false
}
