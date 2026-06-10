package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var dryrun *bool
var labelKey, labelValue *string
var deleteAge *int

func main() {
	dryrun = flag.Bool("dryrun", false, "wether pods should be terminated")
	labelKey = flag.String("labelKey", "", "the labelKey for targeted namespaces")
	labelValue = flag.String("labelValue", "", "the labelValue for targeted namespaces")
	deleteAge = flag.Int("deleteAge", 604800, "the min age in seconds for terminated pods to be targeted")
	flag.Parse()
	// Create Kubernetes client
	clientset, err := getKubernetesClient()
	if err != nil {
		log.Fatalf("error initializing Kubernetes client: %v", err)
	}

	log.Println("Starting pod deletion task...")
	if err := terminateOldPods(clientset); err != nil {
		log.Printf("Error deleting old pods: %v", err)
	} else {
		log.Println("Successfully erased all terminated pods.")
	}
}

func getKubernetesClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(fmt.Errorf("failed to create Kubernetes client: %v", err))
	}
	return kubernetes.NewForConfig(config)
}

// only delete old pods in specific labeled namespaces
func terminateOldPods(clientset *kubernetes.Clientset) error {

	currentTime := time.Now()

	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}
	localCurrentTime := time.Now().In(loc)

	// Get all namespaces
	namespaces, err := clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %v", err)
	}
	for _, namespace := range namespaces.Items {
		describedNs, err := clientset.CoreV1().Namespaces().Get(context.TODO(), namespace.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to describe namespace %s: %v", namespace.Name, err)
		}
		// check if the target label exists and parse the value in variable
		targetValue, exists := describedNs.Labels[*labelKey]
		if exists {
			if targetValue == *labelValue {
				handlePodsInNamespace(clientset, *describedNs, currentTime, localCurrentTime)
			}
		}
	}
	return nil
}

func handlePodsInNamespace(clientset *kubernetes.Clientset, namespace v1.Namespace, currentTime time.Time, localCurrentTime time.Time) error {

	//get all pods in current namespace
	pods, err := clientset.CoreV1().Pods(namespace.Name).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods in namespace %s: %v", namespace.Name, err)
	}
	for _, pod := range pods.Items {
		// check if pod is terminated
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			log.Println("Pod %s in namespace %s is terminated", pod.Name, namespace.Name)
			terminatedAt := pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt
			//iterate all container to get the latest termination age
			for _, container := range pod.Status.ContainerStatuses {
				if container.State.Terminated.FinishedAt.After(terminatedAt.Time) {
					terminatedAt = container.State.Terminated.FinishedAt
				}
			}
			//get the age of the termination in seconds
			ageSeconds := int(time.Since(terminatedAt.Time))

			if ageSeconds > *deleteAge {
				// delete Pod
				if *dryrun {
					log.Println("Dryrun: would delete pod %s in namespace %s", pod.Name, namespace.Name)
				} else {
					err = clientset.CoreV1().Pods(namespace.Name).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						return fmt.Errorf("failed to delete pod %s in namespace %s: %v", pod.Name, namespace.Name, err)
					}
				}
			}
		}
	}

	return nil
}