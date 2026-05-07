package main

import "fmt"

func double(x int) int {
	defer fmt.Println("in double, x =", x)
	return x * 2
}

func main() {
	defer fmt.Println("main done")
	result := double(5)
	fmt.Println("result =", result)
}
in