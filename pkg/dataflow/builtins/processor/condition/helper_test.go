package condition

import (
	"encoding/json"
	"testing"
)

func TestCompareEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		// 相同类型比较
		{"int equal", 1, 1, true},
		{"int not equal", 1, 2, false},
		{"float64 equal", 1.5, 1.5, true},
		{"float64 not equal", 1.5, 2.5, false},
		{"string equal", "hello", "hello", true},
		{"string not equal", "hello", "world", false},
		{"bool equal", true, true, true},
		{"bool not equal", true, false, false},

		// int 和 float64 交叉比较（关键测试！）
		{"int(1) == float64(1)", 1, 1.0, true},
		{"int(1) != float64(2)", 1, 2.0, false},
		{"float64(1) == int(1)", 1.0, 1, true},
		{"int(42) == float64(42)", 42, 42.0, true},

		// 不同整数类型
		{"int32 vs int64", int32(10), int64(10), true},
		{"int vs int32", 100, int32(100), true},
		{"int64 vs float32", int64(5), float32(5.0), true},

		// JSON Number 类型
		{"json.Number equal", json.Number("123"), json.Number("123"), true},
		{"json.Number vs float64", json.Number("42"), 42.0, true},
		{"json.Number vs int", json.Number("100"), 100, true},

		// 边界值
		{"zero equal", 0, 0, true},
		{"zero int vs float", 0, 0.0, true},
		{"negative equal", -5, -5.0, true},
		{"large number", 1000000, 1000000.0, true},

		// 不兼容类型
		{"int vs string", 1, "1", false},
		{"string vs int", "1", 1, false},
		{"nil vs int", nil, 1, false},
		{"int vs nil", 1, nil, false},
		{"nil vs nil", nil, nil, true}, // Go 中 nil == nil 是 true
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareEqual(%v (%T), %v (%T)) = %v, want %v",
					tt.a, tt.a, tt.b, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareGreater(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		// 数值比较
		{"int gt", 5, 3, true},
		{"int not gt", 3, 5, false},
		{"int equal not gt", 5, 5, false},
		{"float64 gt", 5.5, 3.5, true},
		{"int vs float64", 5, 3.0, true},
		{"float64 vs int", 5.0, 3, true},

		// 字符串比较
		{"string gt", "zebra", "apple", true},
		{"string not gt", "apple", "zebra", false},

		// 负数
		{"negative gt", -3, -5, true},
		{"negative not gt", -5, -3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareGreater(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareGreater(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareLess(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		// 数值比较
		{"int lt", 3, 5, true},
		{"int not lt", 5, 3, false},
		{"int equal not lt", 5, 5, false},
		{"float64 lt", 3.5, 5.5, true},
		{"int vs float64", 3, 5.0, true},
		{"float64 vs int", 3.0, 5, true},

		// 字符串比较
		{"string lt", "apple", "zebra", true},
		{"string not lt", "zebra", "apple", false},

		// 负数
		{"negative lt", -5, -3, true},
		{"negative not lt", -3, -5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareLess(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareLess(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareGreaterOrEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"int gt", 5, 3, true},
		{"int eq", 5, 5, true},
		{"int not gte", 3, 5, false},
		{"float64 gte", 5.0, 5.0, true},
		{"int vs float64 eq", 5, 5.0, true},
		{"string gte", "zebra", "apple", true},
		{"string eq", "hello", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareGreaterOrEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareGreaterOrEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareLessOrEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"int lt", 3, 5, true},
		{"int eq", 5, 5, true},
		{"int not lte", 5, 3, false},
		{"float64 lte", 5.0, 5.0, true},
		{"int vs float64 eq", 5, 5.0, true},
		{"string lte", "apple", "zebra", true},
		{"string eq", "hello", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareLessOrEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareLessOrEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		ok       bool
	}{
		{"int", 42, 42.0, true},
		{"int64", int64(100), 100.0, true},
		{"int32", int32(50), 50.0, true},
		{"float32", float32(3.14), float64(float32(3.14)), true},
		{"float64", 2.718, 2.718, true},
		{"json.Number", json.Number("123.45"), 123.45, true},
		{"string", "hello", 0, false},
		{"bool", true, 0, false},
		{"nil", nil, 0, false},
		{"slice", []int{1, 2, 3}, 0, false},
		{"map", map[string]int{"a": 1}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64(tt.input)
			if ok != tt.ok {
				t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && result != tt.expected {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBuildValueSet(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected map[interface{}]bool
	}{
		{
			name:     "single value",
			value:    42,
			expected: map[interface{}]bool{42: true},
		},
		{
			name:     "array of values",
			value:    []interface{}{1, 2, 3},
			expected: map[interface{}]bool{1: true, 2: true, 3: true},
		},
		{
			name:     "empty array",
			value:    []interface{}{},
			expected: map[interface{}]bool{},
		},
		{
			name:     "mixed types",
			value:    []interface{}{1, "hello", true},
			expected: map[interface{}]bool{1: true, "hello": true, true: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildValueSet(tt.value)
			if len(result) != len(tt.expected) {
				t.Errorf("buildValueSet() got %d items, want %d", len(result), len(tt.expected))
			}
			for k := range tt.expected {
				if !result[k] {
					t.Errorf("buildValueSet() missing key %v", k)
				}
			}
		})
	}
}

func TestValueInSet(t *testing.T) {
	set := map[interface{}]bool{
		1:   true,
		2:   true,
		1.0: false, // 注意：1 和 1.0 在 map 中是不同的 key
		"hello": true,
	}

	tests := []struct {
		name     string
		val      interface{}
		expected bool
	}{
		{"int in set", 1, true},
		{"int not in set", 3, false},
		{"string in set", "hello", true},
		{"string not in set", "world", false},
		{"float64 matching int", 1.0, true}, // 应该通过类型转换匹配
		{"float64 not in set", 3.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueInSet(tt.val, set)
			if result != tt.expected {
				t.Errorf("valueInSet(%v) = %v, want %v", tt.val, result, tt.expected)
			}
		})
	}
}
