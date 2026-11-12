package demo_test

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	modulePath     = "github.com/DmytroLysenko1/faultline/"
	serverPackage  = "./cmd/faultline"
	dockerfilePath = "deploy/dev/Dockerfile"
	dockerignore   = "deploy/dev/Dockerfile.dockerignore"
	copyPrefix     = "COPY "
	allowPrefix    = "!"
	dirSuffix      = "/"
	goBinary       = "go"
)

func serverSourceDirs(t *testing.T, root string) []string {
	t.Helper()
	list := exec.CommandContext(t.Context(), goBinary, "list", "-deps", "-f", "{{.ImportPath}}", serverPackage)
	list.Dir = root
	var stderr bytes.Buffer
	list.Stderr = &stderr
	out, err := list.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}
	var dirs []string
	for line := range strings.Lines(string(out)) {
		local, found := strings.CutPrefix(strings.TrimSpace(line), modulePath)
		if found {
			dirs = append(dirs, strings.SplitN(local, dirSuffix, 2)[0])
		}
	}
	slices.Sort(dirs)
	return slices.Compact(dirs)
}

func linesOf(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		lines = append(lines, strings.TrimSpace(scanner.Text()))
	}
	return lines
}

func copiedDirs(t *testing.T, path string) map[string]bool {
	t.Helper()
	copied := map[string]bool{}
	for _, line := range linesOf(t, path) {
		fields := strings.Fields(strings.TrimPrefix(line, copyPrefix))
		if strings.HasPrefix(line, copyPrefix) && len(fields) > 0 {
			copied[strings.TrimSuffix(fields[0], dirSuffix)] = true
		}
	}
	return copied
}

func allowedDirs(t *testing.T, path string) map[string]bool {
	t.Helper()
	allowed := map[string]bool{}
	for _, line := range linesOf(t, path) {
		if entry, found := strings.CutPrefix(line, allowPrefix); found {
			allowed[strings.TrimSuffix(entry, dirSuffix)] = true
		}
	}
	return allowed
}

// TestTheImageBuildSeesEveryPackageOfTheServer: a top-level directory the server imports but the
// Dockerfile does not copy builds locally and fails only inside docker build (pkg/ in step 2).
func TestTheImageBuildSeesEveryPackageOfTheServer(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	copied := copiedDirs(t, filepath.Join(root, dockerfilePath))
	allowed := allowedDirs(t, filepath.Join(root, dockerignore))
	for _, dir := range serverSourceDirs(t, root) {
		if !copied[dir] {
			t.Errorf("%s imports %s/, but %s does not COPY it", serverPackage, dir, dockerfilePath)
		}
		if !allowed[dir] {
			t.Errorf("%s imports %s/, but %s does not let it into the build context", serverPackage, dir, dockerignore)
		}
	}
}
