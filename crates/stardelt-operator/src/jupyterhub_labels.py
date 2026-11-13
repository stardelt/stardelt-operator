# Spark compute tiers (internal#161 slice 4c). A single-user pod says whose it is, so a
# per-SparkCluster ingress policy can admit exactly the owner's pods. The groups are
# JupyterHub's, built from the person's Keycloak `groups` claim. A name that cannot be
# a label segment is skipped and logged: the policy then admits nobody from that group,
# which fails closed instead of opening the Spark server.
import re

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$")

def _label_segment(value):
    return value if len(value) <= 63 and _LABEL_NAME.match(value) else None

def _stardelt_labels(spawner, pod):
    # KubeSpawner hands a V1Pod, not a dict (platform#841).
    if pod.metadata is None:
        return pod
    labels = pod.metadata.labels if pod.metadata.labels is not None else {}
    pod.metadata.labels = labels
    user = getattr(spawner.user, "name", "") or ""
    if _label_segment(user):
        labels["stardelt.io/user"] = user
    else:
        spawner.log.warning("notebook pod: username %r cannot be a label, skipped", user)
    for group in getattr(spawner.user, "groups", None) or []:
        name = getattr(group, "name", None) or ""
        if _label_segment("group." + name):
            labels["stardelt.io/group." + name] = "true"
        else:
            spawner.log.warning("notebook pod: group %r cannot be a label, skipped", name)
    return pod

c.KubeSpawner.modify_pod_hook = _stardelt_labels
