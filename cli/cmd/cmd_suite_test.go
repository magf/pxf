package cmd_test

import (
	"os/user"
	"testing"

	"github.com/GreengageDB/gp-common-go-libs/operating"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	testStdout *gbytes.Buffer
	testStderr *gbytes.Buffer
)

func TestCmd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cmd Suite")
}

var _ = BeforeEach(func() {
	_, _, testStdout, testStderr, _ = testhelper.SetupTestEnvironment()
	operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
	operating.System.Hostname = func() (string, error) { return "testHost", nil }
})
