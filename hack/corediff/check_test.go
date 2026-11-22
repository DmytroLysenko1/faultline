package corediff_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	baseTag     = "base"
	listedFile  = "internal/domain/listed.go"
	otherFile   = "pkg/unlisted/unlisted.go"
	testFile    = "internal/domain/listed_test.go"
	harnessDir  = "internal/application/processing/processingtest/fake.go"
	allowName   = "allow"
	renamedFrom = "internal/domain/old/old.go"
	renamedTo   = "internal/domain/moved/moved.go"
	renameLines = 40
	fileMode    = 0o600
	dirMode     = 0o700
)

func script(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("../core-diff-check.sh")
	if err != nil {
		t.Fatalf("script path: %v", err)
	}
	return path
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func write(t *testing.T, dir, name, text string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(text), fileMode); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func repository(t *testing.T, edits ...string) string {
	t.Helper()
	return repositoryWith(t, listedFile+" class\n", edits...)
}

func repositoryWith(t *testing.T, allow string, edits ...string) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q")
	write(t, dir, "README", "base")
	write(t, dir, renamedFrom, strings.Repeat("package x\n", renameLines))
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "base")
	git(t, dir, "tag", baseTag)
	for _, name := range edits {
		write(t, dir, name, "package x")
	}
	write(t, dir, allowName, allow)
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "edit")
	return dir
}

func check(t *testing.T, dir string) (string, error) {
	t.Helper()
	return checkAgainst(t, dir, baseTag)
}

func checkAgainst(t *testing.T, dir, base string) (string, error) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "bash", script(t), base)
	command.Dir = dir
	command.Env = append(os.Environ(), "CORE_DIFF_ALLOW="+allowName)
	out, err := command.CombinedOutput()
	return string(out), err
}

func TestCoreDiffCheckFailsOnAnUnlistedEdit(t *testing.T) {
	out, err := check(t, repository(t, listedFile, otherFile))

	if err == nil {
		t.Fatalf("the check passed an unlisted core edit:\n%s", out)
	}
	if !strings.Contains(out, otherFile) || strings.Contains(out, "  "+listedFile) {
		t.Errorf("output names the wrong files:\n%s", out)
	}
}

func TestCoreDiffCheckPassesListedEditsTestsAndHarnesses(t *testing.T) {
	out, err := check(t, repository(t, listedFile, testFile, harnessDir))

	if err != nil {
		t.Fatalf("the check refused listed edits, tests and test harnesses: %v\n%s", err, out)
	}
}

func TestCoreDiffCheckFailsWithoutItsBase(t *testing.T) {
	out, err := checkAgainst(t, repository(t, listedFile), "no-such-tag")

	if err == nil {
		t.Fatalf("the check passed without the base tag:\n%s", out)
	}
}

func TestCoreDiffCheckSeesARenamedCoreFile(t *testing.T) {
	dir := repository(t, listedFile)
	if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(renamedTo)), dirMode); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	git(t, dir, "mv", renamedFrom, renamedTo)
	write(t, dir, renamedTo, strings.Repeat("package x\n", renameLines)+"var x = 1\n")
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "rename")

	out, err := check(t, dir)

	if err == nil || !strings.Contains(out, renamedTo) {
		t.Fatalf("the check missed a renamed core file: %v\n%s", err, out)
	}
}

func TestCoreDiffCheckRefusesALineWithoutAClass(t *testing.T) {
	out, err := check(t, repositoryWith(t, listedFile+"\n", listedFile))

	if err == nil {
		t.Fatalf("the check passed an allow line without a class:\n%s", out)
	}
}

func TestCoreDiffCheckSeesADeletedCoreFile(t *testing.T) {
	dir := repository(t, listedFile)
	git(t, dir, "rm", "-q", renamedFrom)
	git(t, dir, "commit", "-q", "-m", "delete")

	out, err := check(t, dir)

	if err == nil || !strings.Contains(out, renamedFrom) {
		t.Fatalf("the check missed a deleted core file: %v\n%s", err, out)
	}
}

func TestCoreDiffCheckSeesAPackageWhoseNameEndsInTest(t *testing.T) {
	out, err := check(t, repository(t, listedFile, latestFile))

	if err == nil || !strings.Contains(out, latestFile) {
		t.Fatalf("the check skipped a production package named like a harness: %v\n%s", err, out)
	}
}

const latestFile = "internal/domain/latest/latest.go"
