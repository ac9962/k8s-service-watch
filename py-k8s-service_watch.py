#
# This script watches for changes to Kubernetes services.
# It was inspired by the article https://medium.com/programming-kubernetes/building-stuff-with-the-kubernetes-api-part-3-using-python-aea5ab16f627
import os
from kubernetes.config.config_exception import ConfigException
import requests
from kubernetes import client, config, watch
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')

# set the Consul agent URL and other variables
consul_url = "http://40.113.218.45:8500"
datacenter = "dev"

control_plane_host = "40.113.218.45"
control_plane_ip = "40.113.218.45"

def main(k8s_context=None):
    # setup the namespace
    logging.info(f"Running service for Kubernetes cluster {k8s_context}")
    ns = os.getenv("K8S_NAMESPACE")
    if ns is None:
        ns = ""

    # configure client
    config.load_kube_config(context=k8s_context)
    api = client.CoreV1Api()

    # Setup new watch
    w = watch.Watch()
    logging.info("Watching for Kubernetes services for all namespaces")

    for item in w.stream(api.list_service_for_all_namespaces, timeout_seconds=0):
        svc = item['object']

        # get the metadata labels
        labels = svc.metadata.labels
        # look for a label named "registerWithMesh"
        if not labels:
            logging.info('No label found')
        else:
            try:
                if labels['registerWithMesh'] == "true":
                    register_flag = True
                else:
                    register_flag = False
            except KeyError:
                register_flag = False
                logging.info(f"label: registerWithMesh not found for service {svc.metadata.name}")
            # notify consul about the service
            if register_flag == True:
                notify_consul(svc, item['type'])


# Notify the Consul agent
def notify_consul(service, action):
    if service.spec.type in("NodePort", "ClusterIP", "LoadBalancer"):
        ports = service.spec.ports
        for port in ports:
            #			print "Port", port
            full_name = service.metadata.namespace + "-" + service.metadata.name + "-" +  (port.name if port.name else "")
            if action == 'ADDED':
            # if action == 'DELETED':
                logging.info(f"Registering new service {full_name}")
                # full_consul_url = consul_url + "/v1/catalog/register"
                full_consul_url = consul_url + "/v1/agent/service/register"
                # determine which port to use depending on the service port type
                if service.spec.type == "NodePort":
                    final_host = control_plane_host
                    final_address = control_plane_ip
                    final_port = port.node_port
                if service.spec.type == "ClusterIP":
                    final_host = service.spec.cluster_ip
                    final_address = service.spec.cluster_ip
                    final_port = port.port
                if service.spec.type == "LoadBalancer":
                    final_host = service.status.load_balancer.ingress[0].ip
                    final_address = service.status.load_balancer.ingress[0].ip
                    final_port = port.port

                consul_json = {
                    "ID": service.metadata.name,
                    "Name": full_name,
                    "Tags": [service.metadata.namespace],
                    "Address": final_address,
                    "Port": final_port,
                    # "Meta": {
                    #     "redis_version": "4.0"},
                    "EnableTagOverride": False,
                    "Check": {"DeregisterCriticalServiceAfter": "90m",
                              # "Args": ["/usr/local/bin/check_redis.py"],
                              "HTTP": f"http://{final_address}:{final_port}/health",
                              "Interval": "90s"
                              }
                }
                logging.info(f"request {full_consul_url} {consul_json}")
                html_headers = {"Content-Type": "application/json", "Accept": "application/json"}
                response = requests.put(full_consul_url, json=consul_json, headers=html_headers)
                if response.status_code != 200:
                    logging.info(f"Status: {response.status_code} Headers: {response.headers} Response content: {response.text}")

            if action == 'DELETED':
            # if action == 'ADDED':
                serviceID = service.metadata.name
                logging.info(f"Deregistering {serviceID}")
                full_consul_url = consul_url + "/v1/agent/service/deregister/" + serviceID
                # assemble the Consul API payload
                logging.info(full_consul_url)
                response = requests.put(full_consul_url)
                logging.info(response.status_code)
                if response.status_code != 200:
                    logging.info('Status:', response.status_code, 'Headers:', response.headers, 'Response content:',
                          response.text)
    else:
        logging.info("Skipping service", service.metadata.name, "becuase it is not a NodePort service type")


if __name__ == '__main__':
    main(k8s_context='Surfers2Cluster')
