# PodSecurity "restricted" for the single-user pod (platform#1237). z2jh 4.4.5 has
# no value for the notebook container's securityContext, so KubeSpawner's
# `extra_container_config` (kubespawner 7.1.0, in the hub image) carries the three
# fields the chart leaves out. `extra_container_config` *replaces* the
# securityContext KubeSpawner builds from `singleuser.uid` and
# `singleuser.allowPrivilegeEscalation`, so both are restated from the same traits.
# Nothing functional is lost: the image runs as uid 1000 and needs no capability.
c.KubeSpawner.extra_container_config = {
    "securityContext": {
        "runAsUser": c.KubeSpawner.uid,
        "runAsNonRoot": True,
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
    }
}
