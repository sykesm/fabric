/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kafka

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/hyperledger/fabric/integration/channelparticipation"
	"github.com/hyperledger/fabric/integration/nwo"
	"github.com/hyperledger/fabric/integration/nwo/commands"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("basic kafka network with 2 orgs", func() {
	var (
		testDir   string
		client    *docker.Client
		network   *nwo.Network
		chaincode nwo.Chaincode
		process   ifrit.Process
	)

	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "e2e")
		Expect(err).NotTo(HaveOccurred())

		client, err = docker.NewClientFromEnv()
		Expect(err).NotTo(HaveOccurred())

		chaincode = nwo.Chaincode{
			Name:            "mycc",
			Version:         "0.0",
			Path:            components.Build("github.com/hyperledger/fabric/integration/chaincode/simple/cmd"),
			Lang:            "binary",
			PackageFile:     filepath.Join(testDir, "simplecc.tar.gz"),
			Ctor:            `{"Args":["init","a","100","b","200"]}`,
			SignaturePolicy: `AND ('Org1MSP.member','Org2MSP.member')`,
			Sequence:        "1",
			InitRequired:    true,
			Label:           "my_prebuilt_chaincode",
		}

		network = nwo.New(nwo.BasicKafka(), testDir, client, StartPort(), components)
		network.MetricsProvider = "prometheus"
		network.Consensus.ChannelParticipationEnabled = true
		network.GenerateConfigTree()
		network.Bootstrap()

		networkRunner := network.NetworkGroupRunner()
		process = ifrit.Invoke(networkRunner)
		Eventually(process.Ready(), network.EventuallyTimeout).Should(BeClosed())
	})

	AfterEach(func() {
		if process != nil {
			process.Signal(syscall.SIGTERM)
			Eventually(process.Wait(), network.EventuallyTimeout).Should(Receive())
		}
		if network != nil {
			network.Cleanup()
		}
		os.RemoveAll(testDir)
	})

	It("executes a basic kafka network with 2 orgs (using docker chaincode builds)", func() {
		chaincodePath, err := filepath.Abs("../chaincode/module")
		Expect(err).NotTo(HaveOccurred())

		// use these two variants of the same chaincode to ensure we test
		// the golang docker build for both module and gopath chaincode
		chaincode = nwo.Chaincode{
			Name:            "mycc",
			Version:         "0.0",
			Path:            chaincodePath,
			Lang:            "golang",
			PackageFile:     filepath.Join(testDir, "modulecc.tar.gz"),
			Ctor:            `{"Args":["init","a","100","b","200"]}`,
			SignaturePolicy: `AND ('Org1MSP.member','Org2MSP.member')`,
			Sequence:        "1",
			InitRequired:    true,
			Label:           "my_module_chaincode",
		}

		gopathChaincode := nwo.Chaincode{
			Name:            "mycc",
			Version:         "0.0",
			Path:            "github.com/hyperledger/fabric/integration/chaincode/simple/cmd",
			Lang:            "golang",
			PackageFile:     filepath.Join(testDir, "simplecc.tar.gz"),
			Ctor:            `{"Args":["init","a","100","b","200"]}`,
			SignaturePolicy: `AND ('Org1MSP.member','Org2MSP.member')`,
			Sequence:        "1",
			InitRequired:    true,
			Label:           "my_simple_chaincode",
		}

		orderer := network.Orderer("orderer")

		network.CreateAndJoinChannel(orderer, "testchannel")
		cl := channelparticipation.List(network, orderer)
		channelparticipation.ChannelListMatcher(cl, []string{"testchannel"}, []string{"systemchannel"}...)

		nwo.EnableCapabilities(network, "testchannel", "Application", "V2_0", orderer, network.Peer("Org1", "peer0"), network.Peer("Org2", "peer0"))

		// package, install, and approve by org1 - module chaincode
		packageInstallApproveChaincode(network, "testchannel", orderer, chaincode, network.Peer("Org1", "peer0"))

		// package, install, and approve by org2 - gopath chaincode, same logic
		packageInstallApproveChaincode(network, "testchannel", orderer, gopathChaincode, network.Peer("Org2", "peer0"))

		testPeers := network.PeersWithChannel("testchannel")
		nwo.CheckCommitReadinessUntilReady(network, "testchannel", chaincode, network.PeerOrgs(), testPeers...)
		nwo.CommitChaincode(network, "testchannel", orderer, chaincode, testPeers[0], testPeers...)
		nwo.InitChaincode(network, "testchannel", orderer, chaincode, testPeers...)

		By("listing the containers after committing the chaincode definition")
		initialContainerFilter := map[string][]string{
			"name": {
				chaincodeContainerNameFilter(network, chaincode),
				chaincodeContainerNameFilter(network, gopathChaincode),
			},
		}

		containers, err := client.ListContainers(docker.ListContainersOptions{Filters: initialContainerFilter})
		Expect(err).NotTo(HaveOccurred())
		Expect(containers).To(HaveLen(2))

		RunQueryInvokeQuery(network, orderer, network.Peer("Org1", "peer0"), "testchannel")

		CheckPeerOperationEndpoints(network, network.Peer("Org2", "peer0"))
		CheckOrdererOperationEndpoints(network, orderer)

		// upgrade chaincode to v2.0 with different label
		chaincode.Version = "1.0"
		chaincode.Sequence = "2"
		chaincode.Label = "my_module_chaincode_updated"
		gopathChaincode.Version = "1.0"
		gopathChaincode.Sequence = "2"
		gopathChaincode.Label = "my_simple_chaincode_updated"

		// package, install, and approve by org1 - module chaincode
		packageInstallApproveChaincode(network, "testchannel", orderer, chaincode, network.Peer("Org1", "peer0"))

		// package, install, and approve by org2 - gopath chaincode, same logic
		packageInstallApproveChaincode(network, "testchannel", orderer, gopathChaincode, network.Peer("Org2", "peer0"))

		nwo.CheckCommitReadinessUntilReady(network, "testchannel", chaincode, network.PeerOrgs(), testPeers...)
		nwo.CommitChaincode(network, "testchannel", orderer, chaincode, testPeers[0], testPeers...)
		nwo.InitChaincode(network, "testchannel", orderer, chaincode, testPeers...)

		By("listing the containers after updating the chaincode definition")
		// expect the containers for the previous package id to be stopped
		containers, err = client.ListContainers(docker.ListContainersOptions{Filters: initialContainerFilter})
		Expect(err).NotTo(HaveOccurred())
		Expect(containers).To(HaveLen(0))
		updatedContainerFilter := map[string][]string{
			"name": {
				chaincodeContainerNameFilter(network, chaincode),
				chaincodeContainerNameFilter(network, gopathChaincode),
			},
		}
		containers, err = client.ListContainers(docker.ListContainersOptions{Filters: updatedContainerFilter})
		Expect(err).NotTo(HaveOccurred())
		Expect(containers).To(HaveLen(2))

		RunQueryInvokeQuery(network, orderer, network.Peer("Org1", "peer0"), "testchannel")

		By("retrieving the local mspid of the peer via simple chaincode")
		sess, err := network.PeerUserSession(network.Peer("Org2", "peer0"), "User1", commands.ChaincodeQuery{
			ChannelID: "testchannel",
			Name:      "mycc",
			Ctor:      `{"Args":["mspid"]}`,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess).To(gbytes.Say("Org2MSP"))
	})
})

func RunQueryInvokeQuery(n *nwo.Network, orderer *nwo.Orderer, peer *nwo.Peer, channel string) {
	By("querying the chaincode")
	sess, err := n.PeerUserSession(peer, "User1", commands.ChaincodeQuery{
		ChannelID: channel,
		Name:      "mycc",
		Ctor:      `{"Args":["query","a"]}`,
	})
	Expect(err).NotTo(HaveOccurred())
	Eventually(sess, n.EventuallyTimeout).Should(gexec.Exit(0))
	Expect(sess).To(gbytes.Say("100"))

	sess, err = n.PeerUserSession(peer, "User1", commands.ChaincodeInvoke{
		ChannelID: channel,
		Orderer:   n.OrdererAddress(orderer, nwo.ListenPort),
		Name:      "mycc",
		Ctor:      `{"Args":["invoke","a","b","10"]}`,
		PeerAddresses: []string{
			n.PeerAddress(n.Peer("Org1", "peer0"), nwo.ListenPort),
			n.PeerAddress(n.Peer("Org2", "peer0"), nwo.ListenPort),
		},
		WaitForEvent: true,
	})
	Expect(err).NotTo(HaveOccurred())
	Eventually(sess, n.EventuallyTimeout).Should(gexec.Exit(0))
	Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful. result: status:200"))

	sess, err = n.PeerUserSession(peer, "User1", commands.ChaincodeQuery{
		ChannelID: channel,
		Name:      "mycc",
		Ctor:      `{"Args":["query","a"]}`,
	})
	Expect(err).NotTo(HaveOccurred())
	Eventually(sess, n.EventuallyTimeout).Should(gexec.Exit(0))
	Expect(sess).To(gbytes.Say("90"))
}

func CheckPeerOperationEndpoints(network *nwo.Network, peer *nwo.Peer) {
	logspecURL := fmt.Sprintf("https://127.0.0.1:%d/logspec", network.PeerPort(peer, nwo.OperationsPort))
	healthURL := fmt.Sprintf("https://127.0.0.1:%d/healthz", network.PeerPort(peer, nwo.OperationsPort))

	authClient, unauthClient := nwo.PeerOperationalClients(network, peer)

	CheckLogspecOperations(authClient, logspecURL)
	CheckHealthEndpoint(authClient, healthURL)

	By("getting the logspec without a client cert")
	resp, err := unauthClient.Get(logspecURL)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

	By("ensuring health checks do not require a client cert")
	CheckHealthEndpoint(unauthClient, healthURL)
}

func CheckOrdererOperationEndpoints(network *nwo.Network, orderer *nwo.Orderer) {
	metricsURL := fmt.Sprintf("https://127.0.0.1:%d/metrics", network.OrdererPort(orderer, nwo.OperationsPort))
	logspecURL := fmt.Sprintf("https://127.0.0.1:%d/logspec", network.OrdererPort(orderer, nwo.OperationsPort))
	healthURL := fmt.Sprintf("https://127.0.0.1:%d/healthz", network.OrdererPort(orderer, nwo.OperationsPort))

	authClient, unauthClient := nwo.OrdererOperationalClients(network, orderer)

	CheckOrdererPrometheusMetrics(authClient, metricsURL)
	CheckLogspecOperations(authClient, logspecURL)
	CheckHealthEndpoint(authClient, healthURL)

	By("getting the logspec without a client cert")
	resp, err := unauthClient.Get(logspecURL)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

	By("ensuring health checks do not require a client cert")
	CheckHealthEndpoint(unauthClient, healthURL)
}

func CheckOrdererPrometheusMetrics(client *http.Client, url string) {
	By("hitting the prometheus metrics endpoint")
	resp, err := client.Get(url)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	resp.Body.Close()

	Eventually(getBody(client, url)).Should(ContainSubstring(`# TYPE grpc_server_stream_request_duration histogram`))

	By("checking for some expected metrics")
	body := getBody(client, url)()
	Expect(body).To(ContainSubstring(`# TYPE go_gc_duration_seconds summary`))
	Expect(body).To(ContainSubstring(`# TYPE grpc_server_stream_request_duration histogram`))
	Expect(body).To(ContainSubstring(`grpc_server_stream_request_duration_sum{code="OK",method="Deliver",service="orderer_AtomicBroadcast"`))
	Expect(body).To(ContainSubstring(`grpc_server_stream_request_duration_sum{code="OK",method="Broadcast",service="orderer_AtomicBroadcast"`))
	Expect(body).To(ContainSubstring(`# TYPE grpc_comm_conn_closed counter`))
	Expect(body).To(ContainSubstring(`# TYPE grpc_comm_conn_opened counter`))
	Expect(body).To(ContainSubstring(`ledger_blockchain_height`))
	Expect(body).To(ContainSubstring(`ledger_blockstorage_commit_time_bucket`))
}

func CheckLogspecOperations(client *http.Client, logspecURL string) {
	By("getting the logspec")
	resp, err := client.Get(logspecURL)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	Expect(err).NotTo(HaveOccurred())
	Expect(string(bodyBytes)).To(MatchJSON(`{"spec":"info"}`))

	updateReq, err := http.NewRequest(http.MethodPut, logspecURL, strings.NewReader(`{"spec":"debug"}`))
	Expect(err).NotTo(HaveOccurred())

	By("setting the logspec")
	resp, err = client.Do(updateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
	resp.Body.Close()

	resp, err = client.Get(logspecURL)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	bodyBytes, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	Expect(err).NotTo(HaveOccurred())
	Expect(string(bodyBytes)).To(MatchJSON(`{"spec":"debug"}`))

	By("resetting the logspec")
	updateReq, err = http.NewRequest(http.MethodPut, logspecURL, strings.NewReader(`{"spec":"info"}`))
	Expect(err).NotTo(HaveOccurred())
	resp, err = client.Do(updateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
	resp.Body.Close()
}

func CheckHealthEndpoint(client *http.Client, url string) {
	body := getBody(client, url)()

	var healthStatus healthz.HealthStatus
	err := json.Unmarshal([]byte(body), &healthStatus)
	Expect(err).NotTo(HaveOccurred())
	Expect(healthStatus.Status).To(Equal(healthz.StatusOK))
}

func getBody(client *http.Client, url string) func() string {
	return func() string {
		resp, err := client.Get(url)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		return string(bodyBytes)
	}
}

func packageInstallApproveChaincode(network *nwo.Network, channel string, orderer *nwo.Orderer, chaincode nwo.Chaincode, peers ...*nwo.Peer) {
	nwo.PackageChaincode(network, chaincode, peers[0])
	nwo.InstallChaincode(network, chaincode, peers...)
	nwo.ApproveChaincodeForMyOrg(network, channel, orderer, chaincode, peers...)
}

func hashFile(file string) string {
	f, err := os.Open(file)
	Expect(err).NotTo(HaveOccurred())
	defer f.Close()

	h := sha256.New()
	_, err = io.Copy(h, f)
	Expect(err).NotTo(HaveOccurred())

	return fmt.Sprintf("%x", h.Sum(nil))
}

func chaincodeContainerNameFilter(n *nwo.Network, chaincode nwo.Chaincode) string {
	return fmt.Sprintf("^/%s-.*-%s-%s$", n.NetworkID, chaincode.Label, hashFile(chaincode.PackageFile))
}
