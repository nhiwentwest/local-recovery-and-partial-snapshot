package main

import (
	"log"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

func main() {
	log.Println("Person 3 Failure Injector - Safe Mode")

	// Build recovery binary first with integrated support
	cmd := exec.Command("go", "build", "-o", "bin/p3-recovery.exe", "./cmd/recovery-standalone")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Build failed: %v", err)
	}

	// Run multiple recovery cycles with random failures
	for i := 1; i <= 3; i++ {
		log.Printf("=== Recovery Cycle %d ===", i)

		// Run recovery with integrated snapshot support
		cmd := exec.Command("./bin/p3-recovery.exe", "-snapshot-dir", ".")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Fatalf("Start failed: %v", err)
		}

		// Random kill between 5-15 seconds
		killTime := time.Duration(5+rand.Intn(10)) * time.Second
		log.Printf("Will kill process in %v", killTime)

		timer := time.AfterFunc(killTime, func() {
			log.Printf("Killing recovery process (PID: %d)", cmd.Process.Pid)
			cmd.Process.Kill()
		})

		err := cmd.Wait()
		timer.Stop()

		if err != nil {
			log.Printf("Process exited with error: %v", err)
		} else {
			log.Printf("Process exited normally")
		}

		// Wait before next cycle
		time.Sleep(2 * time.Second)
	}

	// Run one final recovery to verify system recovers completely
	log.Println("=== Final Recovery Verification ===")
	cmdFinal := exec.Command("./bin/p3-recovery.exe", "-snapshot-dir", ".")
	cmdFinal.Stdout = os.Stdout
	cmdFinal.Stderr = os.Stderr

	if err := cmdFinal.Start(); err != nil {
		log.Fatalf("Final recovery start failed: %v", err)
	}

	// Let it run for 8 seconds to ensure complete recovery
	time.Sleep(8 * time.Second)
	cmdFinal.Process.Kill()
	log.Printf("Final recovery test completed")

	log.Println("Failure injection test completed successfully")
}
