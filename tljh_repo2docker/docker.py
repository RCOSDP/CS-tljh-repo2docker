import json
import logging

from urllib.parse import urlparse, quote_plus

from aiodocker import Docker, DockerError

logger = logging.getLogger(__name__)


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
    desired_image_labels = {
        "tljh_repo2docker.display_name": name,
        "tljh_repo2docker.image_name": image_name,
        "tljh_repo2docker.mem_limit": memory,
        "tljh_repo2docker.cpu_limit": cpu,
    }
    optional_label_map = {}
    if optional_labels is not None:
        optional_label_map = {f"tljh_repo2docker.opt.{k}": str(v) for k, v in optional_labels.items()}
        desired_image_labels.update(optional_label_map)

    labels = [f"{key}={value}" for key, value in desired_image_labels.items()]

    builder_labels = {
        "repo2docker.repo": repo,
        "repo2docker.ref": ref,
        "repo2docker.build": image_name,
    }
    builder_labels.update(desired_image_labels)
    builder_labels.update(optional_label_map)

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

    expected_labels = builder_labels.copy()

    async def ensure_image_labels(docker_client):
        try:
            info = await docker_client.images.inspect(image_name)
        except DockerError as err:
            logger.error("Unable to retrieve cached repo2docker image %s", image_name, exc_info=err)
            raise

        current_labels = (info.get("Config", {}) or {}).get("Labels", {}) or {}
        if all(current_labels.get(key) == value for key, value in expected_labels.items()):
            return

        logger.info("Refreshing labels on cached repo2docker image %s", image_name)

        updated_labels = current_labels.copy()
        updated_labels.update(expected_labels)

        repo_part, tag_part = (image_name.split(":", 1) + ["latest"])[:2]
        container = await docker_client.containers.create({"Image": image_name, "Cmd": ["true"]})
        try:
            await container.commit(repository=repo_part, tag=tag_part, config={"Labels": updated_labels})
        finally:
            try:
                await container.delete(force=True)
            except DockerError as cleanup_err:
                logger.warning("Failed to remove temporary repo2docker container for %s", image_name, exc_info=cleanup_err)

    async with Docker() as docker:
        # Skip rebuild if the requested image tag already exists
        try:
            await docker.images.get(image_name)
        except DockerError as e:
            if e.status == 404:
                logger.info(
                    "repo2docker image %s not found locally; building new image", image_name
                )
            else:
                logger.exception("Failed to inspect repo2docker image %s", image_name)
                raise
        else:
            await ensure_image_labels(docker)
            logger.info("Reusing cached repo2docker image %s", image_name)
            return image_name

        logger.info("Starting repo2docker build for %s (ref=%s, image=%s)", repo, ref, image_name)
        await docker.containers.run(config=config)
        return image_name
