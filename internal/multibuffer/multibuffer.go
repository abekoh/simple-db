package multibuffer

import "math"

func bestRoot(available, size int32) int32 {
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	var k int32 = math.MaxInt32
	i := 1.0
	for k > avail {
		i += 1.0
		k = int32(math.Ceil(math.Pow(float64(size), 1.0/i)))
	}
	return k
}

func bestFactor(available, size int32) int32 {
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1.0
	for k > avail {
		i += 1.0
		k = int32(math.Ceil(float64(size) / i))
	}
	return k
}
