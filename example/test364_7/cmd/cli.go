package main

type pprofFlags struct {
	Out string `group:"pprof" help:"output directory" name:"out" placeholder:"directory"`
	CPU bool   `group:"pprof" name:"cpu"`
	Mem bool   `group:"pprof" name:"mem"`
}
