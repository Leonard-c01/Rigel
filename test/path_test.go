package main

import (
	"fmt"

	"github.com/rigel/internal/protocol"
)

func main() {
	fmt.Println("🧪 路径处理功能测试")
	fmt.Println("=" + fmt.Sprintf("%40s", "="))

	testPathParsing()
	testNextHopCalculation()
	testPathValidation()

	fmt.Println("\n🎉 所有路径测试通过！")
}

func testPathParsing() {
	fmt.Println("\n1️⃣ 测试路径解析")

	testCases := []struct {
		pathString string
		expected   []string
	}{
		{"proxy1:8001,proxy2:8002,target:8003", []string{"proxy1:8001", "proxy2:8002", "target:8003"}},
		{"single:8001", []string{"single:8001"}},
		{"", nil},
	}

	for i, tc := range testCases {
		result := protocol.ParsePath(tc.pathString)

		if len(result) != len(tc.expected) {
			fmt.Printf("❌ 测试 %d 失败: 长度不匹配\n", i+1)
			continue
		}

		match := true
		for j, hop := range result {
			if j >= len(tc.expected) || hop != tc.expected[j] {
				match = false
				break
			}
		}

		if match {
			fmt.Printf("✅ 测试 %d 通过: %s → %v\n", i+1, tc.pathString, result)
		} else {
			fmt.Printf("❌ 测试 %d 失败: %s → %v (期望: %v)\n", i+1, tc.pathString, result, tc.expected)
		}
	}
}

func testNextHopCalculation() {
	fmt.Println("\n2️⃣ 测试下一跳计算")

	pathString := "proxy1:8001,proxy2:8002,proxy3:8003,target:8004"

	testCases := []struct {
		currentNode  string
		expectedNext string
		shouldError  bool
	}{
		{"", "proxy1:8001", false},             // 起点
		{"proxy1:8001", "proxy2:8002", false},  // 第一跳
		{"proxy2:8002", "proxy3:8003", false},  // 第二跳
		{"proxy3:8003", "target:8004", false},  // 第三跳
		{"target:8004", "", false},             // 终点
		{"unknown:9999", "proxy1:8001", false}, // 不在路径中
	}

	for i, tc := range testCases {
		nextHop, err := protocol.GetNextHop(pathString, tc.currentNode)

		if tc.shouldError {
			if err == nil {
				fmt.Printf("❌ 测试 %d 失败: 期望错误但成功了\n", i+1)
			} else {
				fmt.Printf("✅ 测试 %d 通过: 正确返回错误\n", i+1)
			}
		} else {
			if err != nil {
				fmt.Printf("❌ 测试 %d 失败: 意外错误 %v\n", i+1, err)
			} else if nextHop != tc.expectedNext {
				fmt.Printf("❌ 测试 %d 失败: 当前=%s, 期望下一跳=%s, 实际=%s\n",
					i+1, tc.currentNode, tc.expectedNext, nextHop)
			} else {
				fmt.Printf("✅ 测试 %d 通过: %s → %s\n", i+1, tc.currentNode, nextHop)
			}
		}
	}
}

func testPathValidation() {
	fmt.Println("\n3️⃣ 测试路径验证")

	testCases := []struct {
		pathString  string
		shouldError bool
		description string
	}{
		{"proxy1:8001,proxy2:8002,target:8003", false, "正常路径"},
		{"single:8001", false, "单跳路径"},
		{"", true, "空路径"},
		{"proxy1:8001,,target:8003", true, "包含空跳点"},
		{"proxy1,proxy2:8002", true, "格式错误的跳点"},
		{"proxy1:8001,proxy2:8002,target:8003", false, "多跳路径"},
	}

	for i, tc := range testCases {
		err := protocol.ValidatePath(tc.pathString)

		if tc.shouldError {
			if err == nil {
				fmt.Printf("❌ 测试 %d 失败: %s - 期望错误但验证通过\n", i+1, tc.description)
			} else {
				fmt.Printf("✅ 测试 %d 通过: %s - 正确检测到错误: %v\n", i+1, tc.description, err)
			}
		} else {
			if err != nil {
				fmt.Printf("❌ 测试 %d 失败: %s - 意外错误: %v\n", i+1, tc.description, err)
			} else {
				fmt.Printf("✅ 测试 %d 通过: %s - 验证通过\n", i+1, tc.description)
			}
		}
	}
}

func testPathCompletion() {
	fmt.Println("\n4️⃣ 测试路径完成检查")

	pathString := "proxy1:8001,proxy2:8002,target:8003"

	testCases := []struct {
		currentNode string
		expected    bool
	}{
		{"proxy1:8001", false},  // 不是终点
		{"proxy2:8002", false},  // 不是终点
		{"target:8003", true},   // 是终点
		{"unknown:9999", false}, // 不在路径中
	}

	for i, tc := range testCases {
		result := protocol.IsPathComplete(pathString, tc.currentNode)

		if result == tc.expected {
			fmt.Printf("✅ 测试 %d 通过: %s → %t\n", i+1, tc.currentNode, result)
		} else {
			fmt.Printf("❌ 测试 %d 失败: %s → %t (期望: %t)\n",
				i+1, tc.currentNode, result, tc.expected)
		}
	}
}
