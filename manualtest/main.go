package main

import (
	"fmt"
)

func main() {
	//fmt.Println(merge([][]int{{1, 3}, {2, 6}, {8, 10}, {15, 18}}))
	//fmt.Println(uniquePaths(3, 7))
	fmt.Println(climbStairs())
}

func climbStairs(n int) int {
	if n == 1 {
		return 1
	}
	dp := make([]int, n, n)
	dp[0] = 1
	dp[1] = 2
	for i := 2; i < n; i++ {
		dp[i] = dp[i-1] + dp[i-2]
	}
	return dp[len(dp)-1]
}

func uniquePaths(m int, n int) int {
	dp := make([][]int, m, m)
	for i := 0; i < m; i++ {
		dp[i] = make([]int, n, n)
		for j := 0; j < n; j++ {
			if i == 0 || j == 0 {
				dp[i][j] = 1
			}
		}
	}
	for i := 1; i < m; i++ {
		for j := 1; j < n; j++ {
			dp[i][j] = dp[i][j-1] + dp[i-1][j]
		}
	}
	//for i := 0; i < m; i++ {
	//	fmt.Println(dp[i])
	//}
	return dp[m-1][n-1]
}
