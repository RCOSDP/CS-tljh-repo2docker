import json

from urllib.parse import urlparse, quote_plus

from aiodocker import Docker


def get_optional_value(object, key):
    labels = object['Labels']
    abskey = f'tljh_repo2docker.opt.provider.{key}'
    if abskey not in labels:
        return None
    return labels[abskey]


def get_spawn_ref(object):
    labels = object['Labels']
    repo = labels["repo2docker.repo"]
    ref = labels["repo2docker.ref"]
    return quote_plus(f'{repo}#{ref}')


async def list_images():
    """
    Retrieve local images built by repo2docker
    """
    async with Docker() as docker:
        r2d_images = await docker.images.list(
            filters=json.dumps({"dangling": ["false"], "label": ["repo2docker.ref"]})
        )
    images = [
        {
            "provider": image["Labels"].get("tljh_repo2docker.opt.provider", None),
            "repo": get_optional_value(image, 'repo') or image["Labels"]["repo2docker.repo"],
            "ref": image["Labels"]["repo2docker.ref"],
            "spawnref": get_spawn_ref(image),
            "image_name": image["Labels"]["tljh_repo2docker.image_name"],
            "display_name": get_optional_value(image, 'display_name') or image["Labels"]["tljh_repo2docker.display_name"],
            "mem_limit": image["Labels"]["tljh_repo2docker.mem_limit"],
            "cpu_limit": image["Labels"]["tljh_repo2docker.cpu_limit"],
            "status": "built",
        }
        for image in r2d_images
        if "tljh_repo2docker.image_name" in image["Labels"]
    ]
    return images


async def list_containers():
    """
    Retrieve the list of local images being built by repo2docker.
    Images are built in a Docker container.
    """
    async with Docker() as docker:
        r2d_containers = await docker.containers.list(
            filters=json.dumps({"label": ["repo2docker.ref"]})
        )
    containers = [
        {
            "provider": container["Labels"].get("tljh_repo2docker.opt.provider", None),
            "repo": get_optional_value(container, 'repo') or container["Labels"]["repo2docker.repo"],
            "ref": container["Labels"]["repo2docker.ref"],
            "spawnref": get_spawn_ref(container),
            "image_name": container["Labels"]["repo2docker.build"],
            "display_name": get_optional_value(container, 'display_name') or container["Labels"]["tljh_repo2docker.display_name"],
            "mem_limit": container["Labels"]["tljh_repo2docker.mem_limit"],
            "cpu_limit": container["Labels"]["tljh_repo2docker.cpu_limit"],
            "status": "building",
        }
        for container in r2d_containers
        if "repo2docker.build" in container["Labels"]
    ]
    return containers


async def build_image(
    repo,
    ref,
    name="",
    memory=None,
    cpu=None,
    username=None,
    password=None,
    extra_buildargs=None,
    repo2docker_image=None,
    optional_envs=None,
    default_image_name=None,
    optional_labels=None,
):
    """
    Build an image given a repo, ref and limits
    """
    ref = ref or "HEAD"
    if len(ref) >= 40:
        ref = ref[:7]

    # default to the repo name if no name specified
    # and sanitize the name of the docker image
    if default_image_name is not None:
        image_name = name = default_image_name
    else:
        name = name or urlparse(repo).path.strip("/")
        name = name.lower().replace("/", "-")
        image_name = f"{name}:{ref}"

    # memory is specified in GB
    memory = f"{memory}G" if memory else ""
    cpu = cpu or ""

    # add extra labels to set additional image properties
    labels = [
        f"tljh_repo2docker.display_name={name}",
        f"tljh_repo2docker.image_name={image_name}",
        f"tljh_repo2docker.mem_limit={memory}",
        f"tljh_repo2docker.cpu_limit={cpu}",
    ]

    builder_labels = {
        "repo2docker.repo": repo,
        "repo2docker.ref": ref,
        "repo2docker.build": image_name,
        "tljh_repo2docker.display_name": name,
        "tljh_repo2docker.mem_limit": memory,
        "tljh_repo2docker.cpu_limit": cpu,
    }
    if optional_labels is not None:
        labels += [f"tljh_repo2docker.opt.{k}={v}" for k, v in optional_labels.items()]
        builder_labels.update(dict([(f"tljh_repo2docker.opt.{k}", v) for k, v in optional_labels.items()]))

    cmd = [
        "jupyter-repo2docker",
        "--ref",
        ref,
        "--user-name",
        "jovyan",
        "--user-id",
        "1100",
        "--no-run",
        "--image-name",
        image_name,
    ]

    for label in labels:
        cmd += ["--label", label]

    for barg in extra_buildargs or []:
        cmd += ["--build-arg", barg]

    cmd.append(repo)
    envs = []
    if optional_envs is not None:
        for k, v in optional_envs.items():
            envs.append(f'{k}={v}')

    config = {
        "Cmd": cmd,
        "Image": repo2docker_image or "quay.io/jupyterhub/repo2docker:main",
        "Labels": builder_labels,
        "Volumes": {
            "/var/run/docker.sock": {
                "bind": "/var/run/docker.sock",
                "mode": "rw",
            }
        },
        "Env": envs,
        "HostConfig": {
            "Binds": ["/var/run/docker.sock:/var/run/docker.sock"],
        },
        "Tty": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "OpenStdin": False,
    }

    if username and password:
        config.update(
            {
                "Env": [f"GIT_CREDENTIAL_ENV=username={username}\npassword={password}"],
            }
        )

    async with Docker() as docker:
        await docker.containers.run(config=config)
