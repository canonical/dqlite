package dqlite_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

/*
func Example() {
	paths, err := createDemoDataDirs()
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %s", err)
		return
	}
	defer removeDemoDataDirs(paths)
	// Output: hello
}
*/

func createDemoDataDirs() ([]string, error) {
	paths := make([]string, 3)
	for i := range paths {
		path, err := ioutil.TempDir("", "dqlite-demo")
		if err != nil {
			return nil, fmt.Errorf("Failed to create temporary directory: %s", err)
		}
		paths[i] = path
	}
	return paths, nil
}

func removeDemoDataDirs(paths []string) {
	for _, path := range paths {
		os.RemoveAll(path)
	}
}

func TestDemo(t *testing.T) {
	return
	paths, err := createDemoDataDirs()
	if err != nil {
		t.Fatal(err)

	}
	//defer removeDemoDataDirs(paths)

	cmd := exec.Command("go", "run", "./testdata/demo.go", "-data", paths[0])
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run demo program: %v\n%v", err, string(out))
	}
	t.Log(string(out))
}
