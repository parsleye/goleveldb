package main

import (
	"bytes"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
)

type TreeNode struct {
	Val   int
	Left  *TreeNode
	Right *TreeNode
}

type ListNode struct {
	Val  int
	Next *ListNode
}

func pre(root *TreeNode) {
	if root == nil || root.Val == wild {
		return
	}
	fmt.Println(root.Val)
	pre(root.Left)
	pre(root.Right)
}
func main() {
	//tree := buildTree([]int{10, 5, -3, 3, 2, wild, 11, 3, -2, wild, 1}, 0)
	//fmt.Println(pathSum(tree, 8))
	//tree = buildTree([]int{5, 4, 8, 11, wild, 13, 4, 7, 2, wild, wild, 5, 1}, 0)
	//fmt.Println(pathSum(tree, 22))
	//fmt.Println(findAnagrams("abab", "ab"))
	fmt.Println(findDisappearedNumbers([]int{4, 3, 2, 7, 8, 2, 3, 1}))
	fmt.Println(findDisappearedNumbers([]int{1, 1}))
	fmt.Println(findDisappearedNumbers([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128}))
	//for i := 1; i <= 128; i++ {
	//	fmt.Printf("%d,", i)
	//}
}

func read() {
	db, err := leveldb.OpenFile("testdb",
		&opt.Options{
			Filter:                 filter.NewBloomFilter(10),
			BlockCacheEvictRemoved: false,
		},
	)
	if err != nil {
		panic(err)
	}
	for {
		i := rand.Intn(300005061)
		key := []byte(fmt.Sprintf("%018d", i))
		//key := make([]byte, 20)
		//rand.Read(key)
		db.Get(key, nil)
	}
	//iter := db.NewIterator(nil, nil)
	//iter.Last()
	//fmt.Println(string(iter.Key()))
}

func write() {
	_ = os.RemoveAll("testdb")
	db, err := leveldb.OpenFile("testdb",
		&opt.Options{
			Filter:                 filter.NewBloomFilter(10),
			BlockCacheEvictRemoved: false,
		},
	)
	if err != nil {
		panic(err)
	}
	//ch := make(chan []byte, 1000)
	//go func() {
	//	for {
	//		k := <-ch
	//		_, err = db.Get(k, nil)
	//		if err != nil {
	//			panic(err)
	//		}
	//		//if rand.Int()%2 == 0 {
	//		//	//key := make([]byte, 20)
	//		//	//rand.Read(key)
	//		//	//db.Get(key, nil)
	//		//} else {
	//		//	_, err = db.Get(k, nil)
	//		//	if err != nil {
	//		//		panic(err)
	//		//	}
	//		//}
	//
	//	}
	//}()
	var size int
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%020d", i))
		//time.Sleep(time.Millisecond * 10)
		value := make([]byte, 128)
		rand.Read(value)
		db.Put(key, value, nil)
		size += len(key) + len(value)
		if size > 5*opt.GiB {
			break
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func decodeString(s string) string {
	var stack []byte
	for i := 0; i < len(s); i++ {
		stack = append(stack, s[i])
		if stack[len(stack)-1] == ']' {
			var j, k = -1, -1
			for j = len(stack) - 3; j >= 0; j-- {
				if isNumber(stack[j]) {
					if k == -1 {
						k = j
					}
					if j == 0 {
						break
					}
				} else if k != -1 {
					j++
					break
				}
			}
			times, _ := strconv.ParseInt(string(stack[j:k+1]), 10, 64)
			str := stack[k+2 : len(stack)-1]

			stack = stack[:j]
			stack = append(stack, bytes.Repeat(str, int(times))...)
		}
	}
	for i := 0; i < len(stack); i++ {
		if isNumber(stack[i]) {
			return ""
		}
	}
	return string(stack)
}

func isNumber(c byte) bool {
	return c >= '0' && c <= '9'
}

func reconstructQueue(people [][]int) (res [][]int) {
	sort.Slice(people, func(i, j int) bool {
		if people[i][0] == people[j][0] {
			return people[i][1] < people[j][1]
		}
		return people[i][0] > people[j][0]
	})
	res = make([][]int, len(people), len(people))
	for i := 0; i < len(people); i++ {
		at := people[i][1]
		copy(res[at+1:], res[at:])
		res[at] = people[i]
	}
	return
}

func canPartition(nums []int) bool {
	var sum int
	var m = make(map[int]int)
	for i := range nums {
		sum += nums[i]
		m[nums[i]]++
	}
	target := sum / 2
	if target*2 != sum {
		return false
	}
	dp := make([]bool, target+1, target+1)
	for i := 0; i <= target; i++ {
		if i == nums[0] {
			dp[i] = true
		}
		if i > nums[0] {
			break
		}
	}
	//fmt.Println(dp)
	for i := 1; i < len(nums); i++ {
		for j := target; j >= 1; j-- {
			if nums[i] == j {
				dp[j] = true
			} else if j-nums[i] >= 0 && dp[j-nums[i]] {
				dp[j] = true
			}
			if j == target && dp[j] {
				return true
			}
		}
	}
	return false
}

func pathSum(root *TreeNode, targetSum int) (res int) {
	if root == nil {
		return 0
	}
	var dfs func(root *TreeNode, targetSum int) int
	dfs = func(root *TreeNode, targetSum int) int {
		if root == nil {
			return 0
		}
		remain := targetSum - root.Val
		if remain == 0 {
			return 1
		}
		return dfs(root.Left, remain) + dfs(root.Right, remain)
	}
	var pre func(root *TreeNode)
	pre = func(root *TreeNode) {
		if root == nil {
			return
		}
		res += dfs(root, targetSum)
		pre(root.Left)
		pre(root.Right)
	}
	pre(root)
	return
}

const wild = math.MaxInt64

func buildTree(nums []int, i int) *TreeNode {
	if i >= len(nums) || nums[i] == wild {
		return nil
	}
	t := &TreeNode{Val: nums[i]}
	t.Left = buildTree(nums, 2*i+1)
	t.Right = buildTree(nums, 2*i+2)
	return t
}

func findAnagrams(s string, p string) (res []int) {
	var i, j int
	pt := make(map[byte]int)
	for k := range p {
		pt[p[k]]++
	}
	st := make(map[byte]int)
	isEqual := func() bool {
		if len(pt) != len(st) {
			return false
		}
		for k, v := range pt {
			if st[k] != v {
				return false
			}
		}
		return true
	}
	for j = 0; j < len(p); j++ {
		st[s[j]]++
	}
	for {
		if isEqual() {
			res = append(res, i)
		}
		if j >= len(s) {
			break
		}
		st[s[j]]++
		st[s[i]]--
		if st[s[i]] == 0 {
			delete(st, s[i])
		}
		i++
		j++
	}
	return
}

func findDisappearedNumbers(nums []int) (res []int) {
	n := len(nums)
	numUints := (n + 64) / 64
	bitmap := make([]uint64, numUints, numUints)
	for i := 0; i < n; i++ {
		bitmap[(nums[i]-1)/64] |= 1 << ((nums[i] - 1) % 64)
	}
	for pos := 0; pos < n; pos++ {
		if 1<<(pos%64)&bitmap[pos/64] == 0 {
			res = append(res, pos+1)
		}
	}
	//fmt.Printf("%b\n", bitmap[len(bitmap)-1])
	return res
}
