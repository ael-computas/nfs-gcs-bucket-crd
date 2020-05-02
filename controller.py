import json
import yaml
from kubernetes import client, config, watch
import os
from string import Template, ascii_lowercase, digits
from pathlib import Path
from random import choice

DOMAIN = "cx.ael.local"

class NfsController:
    def __init__(self):
        if 'KUBERNETES_PORT' in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()

        configuration = client.Configuration()
        configuration.assert_hostname = False
        self.api_client = client.api_client.ApiClient(configuration=configuration)
        self.k8s_core_api = client.CoreV1Api(api_client=self.api_client)

    def get_owner_reference(self, owner):
        return [{
            "apiVersion": "v1",
            "blockOwnerDeletion": True,
            "controller": True,
            "kind": owner.get("kind"),
            "name": owner.get("metadata").get("name"),
            "uid": owner.get("metadata").get("uid")
        }]

    def _yaml_template(self, filename, template_subs):
        return yaml.load(Template(Path(filename).read_text()).substitute(template_subs), Loader=yaml.FullLoader)

    def create_nfs_replication_controller(self, target_namespace: str, template_subs: dict, ownerRef: dict):
        nfs_bucket_server_replication_controller = self._yaml_template("yaml_templates/nfs-bucket-server-rc.yaml",
                                                                       template_subs=template_subs)
        nfs_bucket_server_replication_controller["metadata"]["ownerReferences"] = ownerRef
        try:
            resp_rc = self.k8s_core_api.create_namespaced_replication_controller(
                body=nfs_bucket_server_replication_controller, namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def delete_nfs_replication_controller(self, target_namespace: str, template_subs: dict):
        nfs_bucket_server_replication_controller = self._yaml_template("yaml_templates/nfs-bucket-server-rc.yaml",
                                                                       template_subs=template_subs)
        name = nfs_bucket_server_replication_controller.get("metadata").get("name")
        try:
            scale = client.V1Scale()
            scale.spec = client.V1ScaleSpec()
            scale.spec.replicas = 0
            self.k8s_core_api.patch_namespaced_replication_controller_scale(
                name, target_namespace, scale)
            #We should probably wait for the scaling to actually kill the pods
            resp_rc = self.k8s_core_api.delete_namespaced_replication_controller(
                name, namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def create_nfs_service(self, target_namespace: str, template_subs: dict, ownerRef: dict):
        nfs_bucket_server_service = self._yaml_template("yaml_templates/nfs-bucket-server-service.yaml",
                                                        template_subs=template_subs)
        nfs_bucket_server_service["metadata"]["ownerReferences"] = ownerRef
        try:
            resp_service = self.k8s_core_api.create_namespaced_service(
                body=nfs_bucket_server_service, namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def delete_nfs_service(self, target_namespace: str, template_subs: dict):
        nfs_bucket_server_service = self._yaml_template("yaml_templates/nfs-bucket-server-service.yaml",
                                                        template_subs=template_subs)
        name = nfs_bucket_server_service.get("metadata").get("name")
        try:
            resp_service = self.k8s_core_api.delete_namespaced_service(
                name, namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def create_pv(self, template_subs: dict, ownerRef: dict):
        nfs_bucket_pv = self._yaml_template("yaml_templates/nfs-bucket-pv.yaml",
                                            template_subs=template_subs)
        nfs_bucket_pv["metadata"]["ownerReferences"] = ownerRef
        try:
            resp_pv = self.k8s_core_api.create_persistent_volume(body=nfs_bucket_pv)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def delete_pv(self, template_subs: dict):
        nfs_bucket_pv = self._yaml_template("yaml_templates/nfs-bucket-pv.yaml",
                                            template_subs=template_subs)
        name = nfs_bucket_pv.get("metadata").get("name")
        try:
            resp_pv = self.k8s_core_api.delete_persistent_volume(name)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def create_pv_claim(self, target_namespace: str, template_subs: dict, ownerRef: dict):
        nfs_bucket_pvc = self._yaml_template("yaml_templates/nfs-bucket-pvc.yaml",
                                             template_subs=template_subs)
        nfs_bucket_pvc["metadata"]["ownerReferences"] = ownerRef
        try:
            resp_pvc = self.k8s_core_api.create_namespaced_persistent_volume_claim(body=nfs_bucket_pvc,
                                                                                   namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def delete_pv_claim(self, target_namespace: str, template_subs: dict):
        nfs_bucket_pvc = self._yaml_template("yaml_templates/nfs-bucket-pvc.yaml",
                                             template_subs=template_subs)
        name = nfs_bucket_pvc.get("metadata").get("name")
        try:
            resp_pvc = self.k8s_core_api.delete_namespaced_persistent_volume_claim(name,
                                                                                   namespace=target_namespace)
        except Exception as e:
            print(f"{e}")
            return False
        return True

    def handle_nfs_bucket(self, crds, obj):
        metadata = obj.get("metadata")
        if not metadata:
            print("No metadata in object, skipping: {}".format(json.dumps(obj, indent=1)))
            return
        base_name = metadata.get("name")
        target_namespace = metadata.get("namespace")
        obj["spec"]["handled"] = True
        bucket_name = obj["spec"]["bucket"]
        service_account_secret = obj["spec"]["service-account-secret"]
        wanted_name = f"{base_name}-server"
        d = {'serviceAccountSecret': service_account_secret,
             'nfsBucketServerName': wanted_name,
             'nfsBucket': bucket_name}

        ownerRef=self.get_owner_reference(obj)
        self.create_nfs_replication_controller(target_namespace=target_namespace, template_subs=d, ownerRef=ownerRef)
        self.create_nfs_service(target_namespace=target_namespace, template_subs=d, ownerRef=ownerRef)
        self.create_pv(template_subs=d, ownerRef=ownerRef)
        self.create_pv_claim(target_namespace=target_namespace, template_subs=d, ownerRef=ownerRef)

        print(f"Updating: {base_name}")
        crds.replace_namespaced_custom_object(DOMAIN, "v1", target_namespace, "nfsbuckets", base_name, obj)

    def handle_delete_nfs_bucket(self, crds, obj):
        metadata = obj.get("metadata")
        if not metadata:
            print("No metadata in object, skipping: {}".format(json.dumps(obj, indent=1)))
            return
        base_name = metadata.get("name")
        target_namespace = metadata.get("namespace")
        obj["spec"]["handled"] = True
        bucket_name = obj["spec"]["bucket"]
        service_account_secret = obj["spec"]["service-account-secret"]
        wanted_name = f"{base_name}-server"
        d = {'serviceAccountSecret': service_account_secret,
             'nfsBucketServerName': wanted_name,
             'nfsBucket': bucket_name}

        self.delete_nfs_replication_controller(target_namespace=target_namespace, template_subs=d)
        self.delete_nfs_service(target_namespace=target_namespace, template_subs=d)
        self.delete_pv(template_subs=d)
        self.delete_pv_claim(target_namespace=target_namespace, template_subs=d)

        print(f"Deleting: {base_name}")
        #crds.replace_namespaced_custom_object(DOMAIN, "v1", target_namespace, "nfsbuckets", base_name, obj)
        #crds.delete_namespaced_custom_object(DOMAIN, "v1", target_namespace, "nfsbuckets", base_name)

    def verify_can_run(self):
        v1 = client.ApiextensionsV1beta1Api(self.api_client)
        current_crds = [x['spec']['names']['kind'].lower() for x in
                        v1.list_custom_resource_definition().to_dict()['items']]
        if 'nfsbucket' not in current_crds:
            print("You need to create the CRD with kubectl apply -f nfs-crd.yaml")
            raise RuntimeError("CRD does not exist in kubernetes cluster, controller can not start!")

    def run_forever(self):
        self.verify_can_run()
        resource_version = ''
        crds = client.CustomObjectsApi(api_client=self.api_client)
        print("Waiting for nfsbuckets to come up...")
        while True:
            stream = watch.Watch().stream(crds.list_cluster_custom_object, DOMAIN, "v1", "nfsbuckets",
                                          resource_version=resource_version)
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
                if 'DELETED' == operation:
                    # TODO: handle delete
                    #print("DELETE is not handled yet.")
                    self.handle_delete_nfs_bucket(crds, obj)
                    continue
                done = spec.get("handled", False)
                if done:
                    print("Already handled.")
                    continue
                self.handle_nfs_bucket(crds, obj)\


if __name__ == "__main__":
    controller = NfsController()
    controller.run_forever()

