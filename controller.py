import json
import yaml
from kubernetes import client, config, watch
import os

DOMAIN = "cx.ael.local"

def handle_nfs_bucket(crds, obj):
    metadata = obj.get("metadata")
    if not metadata:
        print("No metadata in object, skipping: {}".format(json.dumps(obj, indent=1)))
        return
    name = metadata.get("name")
    namespace = metadata.get("namespace")
    obj["spec"]["handled"] = True
    bucket_name = obj["spec"]["bucket"]

    # TODO: do magic.

    print(f"Updating: {name}")
    crds.replace_namespaced_custom_object(DOMAIN, "v1", namespace, "nfsbuckets", name, obj)


if __name__ == "__main__":
    if 'KUBERNETES_PORT' in os.environ:
        config.load_incluster_config()
        definition = 'nfs-crd.yml'
    else:
        config.load_kube_config()
        definition = 'nfs-crd.yml'
    api = client.CustomObjectsApi()

    configuration = client.Configuration()
    configuration.assert_hostname = False
    api_client = client.api_client.ApiClient(configuration=configuration)
    v1 = client.ApiextensionsV1beta1Api(api_client)
    current_crds = [x['spec']['names']['kind'].lower() for x in v1.list_custom_resource_definition().to_dict()['items']]
    if 'nfsbucket' not in current_crds:
        print("You need to create the CRD with kubectl apply -f nfs-crd.yaml")
        os._exit(-1)
    else:
        print("nfskubernetes CRD exists - controller can start!")
    crds = client.CustomObjectsApi(api_client)

    print("Waiting for nfsbuckets to come up...")
    resource_version = ''
    while True:
        stream = watch.Watch().stream(crds.list_cluster_custom_object, DOMAIN, "v1", "nfsbuckets", resource_version=resource_version)
        for event in stream:
            obj = event["object"]
            operation = event['type']
            spec = obj.get("spec")
            if not spec:
                continue
            metadata = obj.get("metadata")
            resource_version = metadata['resourceVersion']
            name = metadata['name']
            print(f"Handling {operation} on {name}")
            done = spec.get("handled", False)
            if done:
                print("Already handled.")
                continue
            handle_nfs_bucket(crds, obj)
