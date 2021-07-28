package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/windows/svc"
)

// Exists reports whether the named file or directory exists.
func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func usage(errmsg string) {
	fmt.Fprintf(os.Stderr,
		"%s\n\n"+
			"usage: %s <command>\n"+
			"       where <command> is one of\n"+
			"       install, remove, debug, start, stop, pause or continue.\n",
		errmsg, os.Args[0])
	os.Exit(2)
}

func main() {
	const svcName = "EN_Servce"

	fmt.Printf("%q\n", os.Args)

	extension := filepath.Ext(os.Args[0])
	fmt.Printf("Program %s Extension=%s\n", os.Args[0], extension)

	cfgfile := strings.Replace(os.Args[0], extension, ".cfg", -1)
	if extension == "" {
		cfgfile = "EN_Servce.cfg"
	}
	if Exists(cfgfile) {
		Cfg.Load(cfgfile)
	}

	fmt.Println("cfgfile : ", cfgfile)

	fmt.Printf("En services...\n")

	fmt.Printf("OS : %s\n", runtime.GOOS)

	inService, err := svc.IsWindowsService()
	if err != nil {
		log.Fatalf("failed to determine if we are running in service: %v", err)
	}
	if inService {
		runService(svcName, false)
		return
	}

	fmt.Printf("inService %v\n", inService)
	//	runService("proba", true)
	if len(os.Args) < 2 {
		usage("no command specified")
	}

	cmd := strings.ToLower(os.Args[1])
	fmt.Printf("cmd=%s\n", cmd)
	//cmd := "debug"
	switch cmd {
	case "debug":
		runService(svcName, true)
		return
	case "install":
		err = installService(svcName, "EN service")
	case "remove":
		err = removeService(svcName)
	case "start":
		err = startService(svcName)
	case "stop":
		err = controlService(svcName, svc.Stop, svc.Stopped)
	case "pause":
		err = controlService(svcName, svc.Pause, svc.Paused)
	case "continue":
		err = controlService(svcName, svc.Continue, svc.Running)
	default:
		usage(fmt.Sprintf("invalid command %s", cmd))
	}
	if err != nil {
		log.Fatalf("failed to %s %s: %v", cmd, svcName, err)
	}

}
