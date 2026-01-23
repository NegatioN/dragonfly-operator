/*
Copyright 2023 DragonflyDB authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/controller"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// getKeyCountFromInfo extracts the number of keys from INFO keyspace output
// Expected format: "db0:keys=69453,expires=0,..."
func getKeyCountFromInfo(ctx context.Context, rc *redis.Client) (int, error) {
	info, err := rc.Info(ctx, "keyspace").Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get keyspace info: %w", err)
	}

	// Parse the keyspace info to extract key count
	// Format: db0:keys=69453,expires=0,...
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "db0:keys=") {
			parts := strings.Split(line, ",")
			if len(parts) > 0 {
				keysPart := strings.TrimPrefix(parts[0], "db0:keys=")
				var keyCount int
				_, err := fmt.Sscanf(keysPart, "%d", &keyCount)
				if err != nil {
					return 0, fmt.Errorf("failed to parse key count from '%s': %w", keysPart, err)
				}
				return keyCount, nil
			}
		}
	}

	return 0, fmt.Errorf("no db0 keyspace found in INFO output")
}

var _ = Describe("Race Condition: Pod Ready Without Role", Ordered, FlakeAttempts(3), func() {
	ctx := context.Background()
	name := "race-test"
	namespace := "default"
	replicas := 2              // Minimal setup: 1 master + 1 replica
	expectedKeyCount := 100000 // Number of keys we write in the test

	df := dfv1alpha1.Dragonfly{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: dfv1alpha1.DragonflySpec{
			Replicas: int32(replicas),
		},
	}

	Context("Testing race condition when pod is ready but lacks role", func() {
		It("Should create Dragonfly with 2 replicas", func() {
			err := k8sClient.Create(ctx, &df)
			Expect(err).To(BeNil())
		})

		It("Should reach ready state with 1 master and 1 replica", func() {
			waitForDragonflyPhase(ctx, k8sClient, name, namespace, controller.PhaseResourcesCreated, 2*time.Minute)
			waitForStatefulSetReady(ctx, k8sClient, name, namespace, 2*time.Minute)
			err := waitForDragonflyPhase(ctx, k8sClient, name, namespace, controller.PhaseReady, 1*time.Minute)
			Expect(err).To(BeNil())

			// Verify we have 1 master and 1 replica
			var pods corev1.PodList
			err = k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
				resources.DragonflyNameLabelKey:    name,
				resources.KubernetesPartOfLabelKey: "dragonfly",
			})
			Expect(err).To(BeNil())
			Expect(pods.Items).To(HaveLen(replicas))

			masterCount := 0
			replicaCount := 0
			for _, pod := range pods.Items {
				role, ok := pod.Labels[resources.RoleLabelKey]
				Expect(ok).To(BeTrue(), fmt.Sprintf("Pod %s should have a role label", pod.Name))

				if role == resources.Master {
					masterCount++
				} else if role == resources.Replica {
					replicaCount++
				}
			}
			Expect(masterCount).To(Equal(1), "Should have exactly 1 master")
			Expect(replicaCount).To(Equal(1), "Should have exactly 1 replica")
		})

		It("Should have PDB configured with MaxUnavailable=1 after system stabilizes", func() {
			// The system starts with PDB potentially at MaxUnavailable=2 during initial setup
			// as replicas establish replication. Wait for system to fully stabilize.
			Eventually(func() int32 {
				var pdb policyv1.PodDisruptionBudget
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}, &pdb)
				if err != nil {
					return -1
				}
				return pdb.Spec.MaxUnavailable.IntVal
			}, 30*time.Second, 1*time.Second).Should(Equal(int32(1)), "PDB should be MaxUnavailable=1 when system is stable")
		})

		It("Should write test data to master that needs replication", func() {
			By("Connecting to master via port-forward")
			stopChan := make(chan struct{}, 1)
			rc, err := checkAndK8sPortForwardRedis(ctx, clientset, cfg, stopChan, name, namespace, "", 6391)
			Expect(err).To(BeNil())
			defer close(stopChan)
			defer rc.Close()

			By("Writing test data to master")
			// Write multiple keys to ensure there's real data to replicate
			timestamp := time.Now().Unix()
			for i := 0; i < expectedKeyCount; i++ {
				key := fmt.Sprintf("test-key-%d", i)
				value := fmt.Sprintf("test-value-%d-timestamp-%d", i, timestamp)
				err := rc.Set(ctx, key, value, 0).Err()
				Expect(err).To(BeNil())
			}

			By(fmt.Sprintf("Data successfully written: %d keys", expectedKeyCount))
		})

		It("Should dynamically adjust PDB during pod deletion to prevent cascading failures", func() {
			By("Step 1: Ensuring system is stable before test (MaxUnavailable=1)")
			// Wait for system to be in normal mode before starting the test
			Eventually(func() int {
				var pdb policyv1.PodDisruptionBudget
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}, &pdb)
				if err != nil {
					return -1
				}
				return int(pdb.Spec.MaxUnavailable.IntVal)
			}, 30*time.Second, 1*time.Second).Should(Equal(1), "System should be stable with MaxUnavailable=1 before test")

			var initialPDB policyv1.PodDisruptionBudget
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, &initialPDB)
			Expect(err).To(BeNil())
			initialMaxUnavailable := int(initialPDB.Spec.MaxUnavailable.IntVal)
			By(fmt.Sprintf("Initial PDB confirmed: MaxUnavailable=%d", initialMaxUnavailable))

			By("Step 2: Finding and deleting a replica pod (wait for role to be assigned)")
			var podToDelete *corev1.Pod

			// Wait for a replica pod to exist with role label assigned
			// After previous test or between retries, pods may be reconciling
			Eventually(func() bool {
				var pods corev1.PodList
				err := k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
					resources.DragonflyNameLabelKey:    name,
					resources.KubernetesPartOfLabelKey: "dragonfly",
				})
				if err != nil {
					return false
				}

				By(fmt.Sprintf("Found %d dragonfly pods", len(pods.Items)))
				for i := range pods.Items {
					role := pods.Items[i].Labels[resources.RoleLabelKey]
					By(fmt.Sprintf("Pod %s has role=%s", pods.Items[i].Name, role))
					if role == resources.Replica {
						podToDelete = &pods.Items[i]
						return true
					}
				}
				return false
			}, 30*time.Second, 1*time.Second).Should(BeTrue(), "Should eventually have a replica pod with role label")

			Expect(podToDelete).NotTo(BeNil(), "Should have found a replica pod to delete")

			By(fmt.Sprintf("Deleting replica pod: %s", podToDelete.Name))
			err = k8sClient.Delete(ctx, podToDelete)
			Expect(err).To(BeNil())

			By("Step 3: Checking if PDB immediately increases to MaxUnavailable=2 (protection mode)")
			pdbIncreased := false
			maxPDBValue := initialMaxUnavailable

			// Poll for 10 seconds to see if operator reacts quickly
			for i := 0; i < 50; i++ {
				var currentPDB policyv1.PodDisruptionBudget
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}, &currentPDB)
				if err == nil {
					currentMaxUnavailable := int(currentPDB.Spec.MaxUnavailable.IntVal)
					if currentMaxUnavailable > maxPDBValue {
						maxPDBValue = currentMaxUnavailable
					}
					if currentMaxUnavailable >= 2 {
						pdbIncreased = true
						By(fmt.Sprintf("PDB increased to MaxUnavailable=%d (protection mode activated)", currentMaxUnavailable))
						break
					}
				}
				time.Sleep(200 * time.Millisecond)
			}

			// We EXPECT this to fail currently
			Expect(pdbIncreased).To(BeTrue(), "PDB should increase to MaxUnavailable=2 during recovery to prevent cascading failures")

			By("Step 4: Waiting for pod to be recreated and replication to stabilize")
			Eventually(func() bool {
				var pods corev1.PodList
				err := k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
					resources.DragonflyNameLabelKey:    name,
					resources.KubernetesPartOfLabelKey: "dragonfly",
				})
				if err != nil {
					return false
				}

				// Check if we have N pods that are ready with roles (not terminating)
				readyWithRole := 0
				for _, pod := range pods.Items {
					// Skip terminating pods
					if pod.DeletionTimestamp != nil {
						continue
					}

					role, hasRole := pod.Labels[resources.RoleLabelKey]
					if hasRole && (role == resources.Master || role == resources.Replica) {
						for _, cond := range pod.Status.Conditions {
							if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
								readyWithRole++
								break
							}
						}
					}
				}
				return readyWithRole >= replicas
			}, 60*time.Second, 2*time.Second).Should(BeTrue(), "Should have all pods ready with roles")

			By("Step 5: Verifying PDB returns to MaxUnavailable=1 (normal mode)")
			Eventually(func() int {
				var finalPDB policyv1.PodDisruptionBudget
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}, &finalPDB)
				if err != nil {
					return -1
				}
				return int(finalPDB.Spec.MaxUnavailable.IntVal)
			}, 30*time.Second, 2*time.Second).Should(Equal(1), "PDB should return to MaxUnavailable=1 after recovery")
		})

		It("Should verify data integrity and replication after recovery", func() {
			By("Step 1: Finding and deleting a replica pod to trigger recovery")
			var pods corev1.PodList
			err := k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
				resources.DragonflyNameLabelKey:    name,
				resources.KubernetesPartOfLabelKey: "dragonfly",
			})
			Expect(err).To(BeNil())

			var replicaToDelete *corev1.Pod
			for i := range pods.Items {
				role := pods.Items[i].Labels[resources.RoleLabelKey]
				if role == resources.Replica {
					replicaToDelete = &pods.Items[i]
					break
				}
			}
			Expect(replicaToDelete).NotTo(BeNil(), "Should have a replica pod to delete")

			By(fmt.Sprintf("Deleting replica pod: %s", replicaToDelete.Name))
			err = k8sClient.Delete(ctx, replicaToDelete)
			Expect(err).To(BeNil())

			By("Step 2: Waiting for pod to be recreated and stable")
			Eventually(func() bool {
				var pods corev1.PodList
				err := k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
					resources.DragonflyNameLabelKey:    name,
					resources.KubernetesPartOfLabelKey: "dragonfly",
				})
				if err != nil {
					return false
				}

				readyWithRole := 0
				for _, pod := range pods.Items {
					if pod.DeletionTimestamp != nil {
						continue
					}
					role, hasRole := pod.Labels[resources.RoleLabelKey]
					if hasRole && (role == resources.Master || role == resources.Replica) {
						for _, cond := range pod.Status.Conditions {
							if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
								readyWithRole++
								break
							}
						}
					}
				}
				return readyWithRole >= replicas
			}, 60*time.Second, 2*time.Second).Should(BeTrue(), "Should have all pods ready with roles after recovery")

			By("Step 3: Finding the recovered replica pod")
			err = k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
				resources.DragonflyNameLabelKey:    name,
				resources.KubernetesPartOfLabelKey: "dragonfly",
			})
			Expect(err).To(BeNil())

			var masterPod, replicaPod *corev1.Pod
			for i := range pods.Items {
				role := pods.Items[i].Labels[resources.RoleLabelKey]
				if role == resources.Master {
					masterPod = &pods.Items[i]
				} else if role == resources.Replica {
					replicaPod = &pods.Items[i]
				}
			}
			Expect(masterPod).NotTo(BeNil(), "Should have a master pod")
			Expect(replicaPod).NotTo(BeNil(), "Should have a replica pod")

			stopChan := make(chan struct{}, 1)
			rc, err := checkAndK8sPortForwardRedis(ctx, clientset, cfg, stopChan, name, namespace, "", 6391)
			Expect(err).To(BeNil())
			defer close(stopChan)
			defer rc.Close()

			By("Verifying original data is still present on master")
			// Check key count using INFO keyspace
			replicationCheckStartTime := time.Now()
			masterKeyCount, err := getKeyCountFromInfo(ctx, rc)
			Expect(err).To(BeNil())
			By(fmt.Sprintf("Master has %d keys (expected %d)", masterKeyCount, expectedKeyCount))
			Expect(masterKeyCount).To(Equal(expectedKeyCount), fmt.Sprintf("Master should have all %d test keys", expectedKeyCount))

			By("Checking master replication status (with retry for replica reconnection)")
			// After pod recovery, replica may need time to reconnect to master
			Eventually(rc.Info(ctx, "replication"), 60*time.Second, 2*time.Second).Should(And(
				ContainSubstring("role:master"),
				ContainSubstring("connected_slaves:1"),
			), "Master should have 1 connected replica within 60 seconds")

			By("Checking replica's persistence info to verify dataset was loaded via replication")
			replicaStopChan := make(chan struct{}, 1)
			defer close(replicaStopChan)

			// Check that the replica has finished loading data
			var loading, loadState string
			Eventually(func() error {
				var err error
				loading, loadState, err = checkPersistenceInfo(ctx, clientset, cfg, replicaPod, replicaStopChan)
				if err != nil {
					return fmt.Errorf("failed to get persistence info from replica: %w", err)
				}
				return nil
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			By(fmt.Sprintf("Replica persistence info: loading=%s, load_state=%s", loading, loadState))

			// Replica should have finished loading (loading=0 means not currently loading)
			Expect(loading).To(Equal("0"), "Replica should have finished loading dataset (loading=0)")

			// load_state should be "done" or empty (empty means no persistence file, which is fine for replicas)
			Expect(loadState).To(Or(Equal("done"), Equal("")), "Replica load_state should be done or empty")

			By("Verifying replica has the replicated data by connecting directly to it")
			// Connect directly to the replica pod to verify it has the data
			pfResult, err := setupPortForwardWithCleanup(ctx, clientset, cfg, replicaPod, resources.DragonflyPort, 30*time.Second)
			Expect(err).To(BeNil())
			defer pfResult.Cleanup()

			replicaClient := redis.NewClient(&redis.Options{
				Addr:         fmt.Sprintf("localhost:%d", pfResult.LocalPort),
				DialTimeout:  15 * time.Second,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 10 * time.Second,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode: maintnotifications.ModeDisabled,
				},
			})
			defer replicaClient.Close()

			// Verify the replica has the test data we wrote earlier
			By("Checking replicated key count on replica using INFO keyspace")
			replicaKeyCount, err := getKeyCountFromInfo(ctx, replicaClient)
			Expect(err).To(BeNil())
			By(fmt.Sprintf("Replica has %d keys (expected %d)", replicaKeyCount, expectedKeyCount))
			Expect(replicaKeyCount).To(Equal(expectedKeyCount), fmt.Sprintf("Replica should have all %d test keys replicated from master", expectedKeyCount))

			By("Checking replica's replication status (with retry)")
			// Replica may need time to establish connection to master
			Eventually(replicaClient.Info(ctx, "replication"), 60*time.Second, 2*time.Second).Should(And(
				Or(
					ContainSubstring("role:slave"), // DragonflyDB uses "slave" for Redis compatibility
					ContainSubstring("role:replica"),
				),
				ContainSubstring("master_link_status:up"),
			), "Replica should connect to master within 60 seconds")

			By("Writing new data to master to verify live replication")
			testKey := fmt.Sprintf("post-recovery-key-%d", time.Now().Unix())
			testValue := "post-recovery-value"
			err = rc.Set(ctx, testKey, testValue, 0).Err()
			Expect(err).To(BeNil())

			// Verify new data appears on replica (give replication a moment)
			Eventually(replicaClient.Get(ctx, testKey), 5*time.Second, 500*time.Millisecond).Should(Equal(testValue), "New data should replicate to replica")

			recoveryDuration := time.Since(replicationCheckStartTime)
			By(fmt.Sprintf("✅ Data integrity and replication verified: %d keys on both master and replica, live replication working", expectedKeyCount))
			By(fmt.Sprintf("⏱️  Total recovery and stabilization time: %.2f seconds", recoveryDuration.Seconds()))
		})

		It("Cleanup", func() {
			var df dfv1alpha1.Dragonfly
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, &df)
			Expect(err).To(BeNil())

			err = k8sClient.Delete(ctx, &df)
			Expect(err).To(BeNil())
		})
	})
})
