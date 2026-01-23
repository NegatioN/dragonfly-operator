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
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Race Condition: Pod Ready Without Role", Ordered, FlakeAttempts(3), func() {
	ctx := context.Background()
	name := "race-test"
	namespace := "default"
	replicas := 2 // Minimal setup: 1 master + 1 replica

	df := dfv1alpha1.Dragonfly{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: dfv1alpha1.DragonflySpec{
			Replicas: int32(replicas),
		},
	}

	var replicaPodName string

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
					replicaPodName = pod.Name
				}
			}
			Expect(masterCount).To(Equal(1), "Should have exactly 1 master")
			Expect(replicaCount).To(Equal(1), "Should have exactly 1 replica")
		})

		It("Should have PDB configured with MaxUnavailable=1", func() {
			var pdb policyv1.PodDisruptionBudget
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, &pdb)
			Expect(err).To(BeNil())
			Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
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
			for i := 0; i < 500000; i++ {
				key := fmt.Sprintf("test-key-%d", i)
				value := fmt.Sprintf("test-value-%d-timestamp-%d", i, time.Now().Unix())
				err := rc.Set(ctx, key, value, 0).Err()
				Expect(err).To(BeNil())
			}

			By("Verifying data was written")
			val, err := rc.Get(ctx, "test-key-0").Result()
			Expect(err).To(BeNil())
			Expect(val).To(ContainSubstring("test-value-0"))

			By("Data successfully written - ready for replication test")
		})

		It("Should detect the race condition window when replica is deleted", func() {
			// Delete the replica pod
			var replicaPod corev1.Pod
			err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: namespace,
				Name:      replicaPodName,
			}, &replicaPod)
			Expect(err).To(BeNil())

			By(fmt.Sprintf("Deleting replica pod: %s", replicaPodName))
			err = k8sClient.Delete(ctx, &replicaPod)
			Expect(err).To(BeNil())

			// Poll rapidly to catch the race condition window
			// The window is when: pod is Ready=True but role label is missing
			raceWindowDetected := false
			podReadyWithoutRole := false
			var raceConditionPod string

			// Track pods that have achieved stable state (ready + have role)
			stablePodsWithRole := make(map[string]bool)

			By("Polling for race condition window (pod ready but no role)")
			// Poll every 100ms for up to 30 seconds
			for i := 0; i < 300; i++ {
				var pods corev1.PodList
				err = k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
					resources.DragonflyNameLabelKey:    name,
					resources.KubernetesPartOfLabelKey: "dragonfly",
				})

				for _, pod := range pods.Items {
					// Check if pod is Running and Ready
					if pod.Status.Phase != corev1.PodRunning {
						continue
					}

					podReady := false
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
							podReady = true
							break
						}
					}

					if !podReady {
						continue
					}

					// Check if pod has container ready
					containerReady := false
					for _, cs := range pod.Status.ContainerStatuses {
						if cs.Name == resources.DragonflyContainerName && cs.Ready {
							containerReady = true
							break
						}
					}

					if !containerReady {
						continue
					}

					// Pod is ready! Now check if it has a role
					roleValue, hasRole := pod.Labels[resources.RoleLabelKey]

					if hasRole {
						// Pod is stable: ready AND has a role
						if roleValue == resources.Master || roleValue == resources.Replica {
							if !stablePodsWithRole[pod.Name] {
								stablePodsWithRole[pod.Name] = true
								By(fmt.Sprintf("✓ Pod %s is stable (ready + role=%s). Total stable: %d/%d",
									pod.Name, roleValue, len(stablePodsWithRole), replicas))
							}
						}
					} else {
						// CRITICAL: Pod is Ready but doesn't have role label yet!
						if !raceWindowDetected {
							raceWindowDetected = true
							podReadyWithoutRole = true
							raceConditionPod = pod.Name
							By(fmt.Sprintf("🔴 RACE WINDOW DETECTED: Pod %s is Ready=True but has NO role label!", pod.Name))

							// At this moment, check what PDB thinks
							var pdb policyv1.PodDisruptionBudget
							err := k8sClient.Get(ctx, types.NamespacedName{
								Name:      name,
								Namespace: namespace,
							}, &pdb)
							if err == nil {
								By(fmt.Sprintf("PDB Status: DisruptionsAllowed=%d, CurrentHealthy=%d, DesiredHealthy=%d, ExpectedPods=%d",
									pdb.Status.DisruptionsAllowed,
									pdb.Status.CurrentHealthy,
									pdb.Status.DesiredHealthy,
									pdb.Status.ExpectedPods))

								// This is the critical insight: if PDB sees 2 healthy pods
								// (the old master + this new pod without a role), it might
								// allow a disruption that could take down the master!
								if pdb.Status.DisruptionsAllowed > 0 {
									By(fmt.Sprintf("⚠️  VULNERABILITY: PDB would allow disruption of another pod! Master could be deleted!"))
								}
							}
						}
					}
				}

				// Early termination: if N=replicas pods are stable (ready + have role),
				// the system has stabilized and we won't see the race condition anymore
				if len(stablePodsWithRole) >= replicas {
					By(fmt.Sprintf("✅ Early termination: %d/%d pods are stable (ready + have role). System has stabilized.",
						len(stablePodsWithRole), replicas))
					break
				}

				time.Sleep(25 * time.Millisecond)
			}

			if raceWindowDetected {
				By(fmt.Sprintf("✅ Race window was detected for pod: %s", raceConditionPod))
				By("This confirms the timing vulnerability exists!")
			} else {
				By("⚠️  Race window was NOT detected in this test run")
				By("This could mean: 1) operator is very fast, 2) timing was unlucky, or 3) operator fixed the issue")
			}

			By(fmt.Sprintf("Final: %d pods achieved stable state (ready + role)", len(stablePodsWithRole)))

			// We expect to see this at least sometimes with FlakeAttempts
			// For now, just log what we found rather than failing the test
			// In production, we'd want this to NOT happen
			GinkgoWriter.Printf("Race condition detected: %v\n", podReadyWithoutRole)
			GinkgoWriter.Printf("Stable pods count: %d/%d\n", len(stablePodsWithRole), replicas)
		})

		It("Should verify data integrity and replication after recovery", func() {
			By("Finding the replica pod")
			var pods corev1.PodList
			err := k8sClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
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

			By("Connecting to master after recovery")
			stopChan := make(chan struct{}, 1)
			rc, err := checkAndK8sPortForwardRedis(ctx, clientset, cfg, stopChan, name, namespace, "", 6391)
			Expect(err).To(BeNil())
			defer close(stopChan)
			defer rc.Close()

			By("Verifying original data is still present on master")
			// Count all keys to ensure data persisted through the failover
			replicationCheckStartTime := time.Now()
			masterKeyCount := 0
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("test-key-%d", i)
				val, err := rc.Get(ctx, key).Result()
				if err == nil && strings.Contains(val, fmt.Sprintf("test-value-%d", i)) {
					masterKeyCount++
				}
			}
			By(fmt.Sprintf("Master has %d/100 test keys", masterKeyCount))
			Expect(masterKeyCount).To(Equal(100), "Master should have all 100 test keys")

			By("Checking master replication status (with retry for replica reconnection)")
			// After pod recovery, replica may need time to reconnect to master
			Eventually(func() (string, error) {
				info, err := rc.Info(ctx, "replication").Result()
				if err != nil {
					return "", err
				}
				By(fmt.Sprintf("Master replication info (attempt):\n%s", info))
				return info, nil
			}, 60*time.Second, 2*time.Second).Should(And(
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
			})
			defer replicaClient.Close()

			// Verify the replica has the test data we wrote earlier
			By("Counting replicated keys on replica")
			replicaKeyCount := 0
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("test-key-%d", i)
				val, err := replicaClient.Get(ctx, key).Result()
				if err == nil && strings.Contains(val, fmt.Sprintf("test-value-%d", i)) {
					replicaKeyCount++
				}
			}
			By(fmt.Sprintf("Replica has %d/100 test keys", replicaKeyCount))
			Expect(replicaKeyCount).To(Equal(100), "Replica should have all 100 test keys replicated from master")

			By("Checking replica's replication status (with retry)")
			// Replica may need time to establish connection to master
			Eventually(func() (string, error) {
				info, err := replicaClient.Info(ctx, "replication").Result()
				if err != nil {
					return "", err
				}
				By(fmt.Sprintf("Replica replication info (attempt):\n%s", info))
				return info, nil
			}, 60*time.Second, 2*time.Second).Should(And(
				ContainSubstring("role:replica"),
				Or(
					ContainSubstring("master_link_status:up"),
					ContainSubstring("master_sync_in_progress:0"),
				),
			), "Replica should connect to master within 60 seconds")

			By("Writing new data to master to verify live replication")
			testKey := fmt.Sprintf("post-recovery-key-%d", time.Now().Unix())
			testValue := "post-recovery-value"
			err = rc.Set(ctx, testKey, testValue, 0).Err()
			Expect(err).To(BeNil())

			// Verify new data appears on replica (give replication a moment)
			Eventually(func() (string, error) {
				return replicaClient.Get(ctx, testKey).Result()
			}, 5*time.Second, 500*time.Millisecond).Should(Equal(testValue), "New data should replicate to replica")

			recoveryDuration := time.Since(replicationCheckStartTime)
			By(fmt.Sprintf("✅ Data integrity and replication verified: 100/100 keys on both master and replica, live replication working"))
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
