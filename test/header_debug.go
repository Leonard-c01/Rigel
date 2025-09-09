package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"tcp-proxy/internal/protocol"
)

func main() {
	fmt.Println("🔍 详细调试头部序列化/反序列化")

	// 创建测试数据
	testData := []byte("Hello, buffered forwarding!")
	fmt.Printf("原始数据: %s (长度: %d)\n", string(testData), len(testData))

	// 创建数据块
	client := protocol.NewBlockProtocolClient()
	block, err := client.EncodeDataWithRoute(testData, "127.0.0.1:8081", 1)
	if err != nil {
		fmt.Printf("❌ 创建数据块失败: %v\n", err)
		return
	}

	fmt.Printf("\n📦 原始数据块头部:\n")
	fmt.Printf("  Magic: 0x%x\n", block.Header.Magic)
	fmt.Printf("  Version: %d\n", block.Header.Version)
	fmt.Printf("  BlockType: %d\n", block.Header.BlockType)
	fmt.Printf("  BlockID: %d\n", block.Header.BlockID)
	fmt.Printf("  BlockSize: %d\n", block.Header.BlockSize)
	fmt.Printf("  TargetAddress: %s\n", block.Header.RouteInfo.TargetAddress)
	fmt.Printf("  Priority: %d\n", block.Header.RouteInfo.Priority)
	fmt.Printf("  TTL: %d\n", block.Header.RouteInfo.TTL)
	fmt.Printf("  Flags: %d\n", block.Header.RouteInfo.Flags)
	fmt.Printf("  Timestamp: %d\n", block.Header.RouteInfo.Timestamp)
	fmt.Printf("  Checksum: 0x%x\n", block.Header.Checksum)
	fmt.Printf("  HeaderCRC: 0x%x\n", block.Header.HeaderCRC)

	// 序列化头部
	headerBytes, err := block.Header.SerializeHeader()
	if err != nil {
		fmt.Printf("❌ 序列化头部失败: %v\n", err)
		return
	}

	fmt.Printf("\n📤 序列化头部信息:\n")
	fmt.Printf("  头部字节长度: %d\n", len(headerBytes))
	fmt.Printf("  前32字节: %x\n", headerBytes[:32])

	// 查看Checksum和HeaderCRC字段的位置
	// 根据结构，Checksum应该在字节位置: 4+1+1+8+4+128+1+1+2+8 = 158
	// HeaderCRC应该在字节位置: 158+4 = 162，但头部只有160字节，所以HeaderCRC在156-159
	checksumPos := 4 + 1 + 1 + 8 + 4 + 128 + 1 + 1 + 2 + 8 // 158
	if checksumPos+4 <= len(headerBytes) {
		checksumBytes := headerBytes[checksumPos : checksumPos+4]
		fmt.Printf("  Checksum字节位置%d: %x\n", checksumPos, checksumBytes)
	}

	headerCRCPos := checksumPos + 4 // 162，但实际应该在156
	if headerCRCPos <= len(headerBytes) {
		// 实际HeaderCRC位置应该在156
		actualHeaderCRCPos := 156
		if actualHeaderCRCPos+4 <= len(headerBytes) {
			headerCRCBytes := headerBytes[actualHeaderCRCPos : actualHeaderCRCPos+4]
			fmt.Printf("  HeaderCRC字节位置%d: %x\n", actualHeaderCRCPos, headerCRCBytes)
		}
	}

	// 手动反序列化进行调试
	fmt.Printf("\n🔍 手动反序列化:\n")
	buf := bytes.NewReader(headerBytes)

	var magic uint32
	var version uint8
	var blockType uint8
	var blockID uint64
	var blockSize uint32

	binary.Read(buf, binary.BigEndian, &magic)
	binary.Read(buf, binary.BigEndian, &version)
	binary.Read(buf, binary.BigEndian, &blockType)
	binary.Read(buf, binary.BigEndian, &blockID)
	binary.Read(buf, binary.BigEndian, &blockSize)

	fmt.Printf("  读取的Magic: 0x%x\n", magic)
	fmt.Printf("  读取的Version: %d\n", version)
	fmt.Printf("  读取的BlockType: %d\n", blockType)
	fmt.Printf("  读取的BlockID: %d\n", blockID)
	fmt.Printf("  读取的BlockSize: %d\n", blockSize)

	// 读取目标地址
	targetBytes := make([]byte, 128)
	buf.Read(targetBytes)
	targetAddress := string(bytes.TrimRight(targetBytes, "\x00"))
	fmt.Printf("  读取的TargetAddress: %s\n", targetAddress)

	var priority uint8
	var ttl uint8
	var flags uint16
	var timestamp int64
	var checksum uint32
	var headerCRC uint32

	binary.Read(buf, binary.BigEndian, &priority)
	binary.Read(buf, binary.BigEndian, &ttl)
	binary.Read(buf, binary.BigEndian, &flags)
	binary.Read(buf, binary.BigEndian, &timestamp)
	binary.Read(buf, binary.BigEndian, &checksum)
	binary.Read(buf, binary.BigEndian, &headerCRC)

	fmt.Printf("  读取的Priority: %d\n", priority)
	fmt.Printf("  读取的TTL: %d\n", ttl)
	fmt.Printf("  读取的Flags: %d\n", flags)
	fmt.Printf("  读取的Timestamp: %d\n", timestamp)
	fmt.Printf("  读取的Checksum: 0x%x\n", checksum)
	fmt.Printf("  读取的HeaderCRC: 0x%x\n", headerCRC)

	// 检查缓冲区位置
	fmt.Printf("  缓冲区剩余字节: %d\n", buf.Len())

	// 使用官方反序列化函数
	fmt.Printf("\n🔍 官方反序列化:\n")
	header, err := protocol.DeserializeHeader(headerBytes)
	if err != nil {
		fmt.Printf("❌ 官方反序列化失败: %v\n", err)
	} else {
		fmt.Printf("  官方Checksum: 0x%x\n", header.Checksum)
		fmt.Printf("  官方BlockSize: %d\n", header.BlockSize)
	}

	// 验证数据校验和
	fmt.Printf("\n🔍 校验和验证:\n")
	fmt.Printf("  原始数据校验和: 0x%x\n", crc32.ChecksumIEEE(testData))
	fmt.Printf("  期望的校验和: 0x%x\n", block.Header.Checksum)
	fmt.Printf("  手动读取的校验和: 0x%x\n", checksum)
}
