import json
import os

from aiodocker import Docker
from aiodocker.exceptions import DockerError
from dockerspawner import DockerSpawner
from docker.errors import APIError
from docker.types import Mount
from jinja2 import BaseLoader, Environment
from jupyter_client.localinterfaces import public_ips
from jupyterhub.traitlets import ByteSpecification
from traitlets import Unicode
from traitlets.config import Configurable
from tornado import web

try:
    from tljh.hooks import hookimpl
except ModuleNotFoundError:
    hookimpl = None

from .docker import list_images
from .token import TokenStore
from .launcher_deprecated import LaunchRedirectHandler

# Default CPU period
# See: https://docs.docker.com/config/containers/resource_constraints/#limit-a-containers-access-to-memory#configure-the-default-cfs-scheduler
CPU_PERIOD = 100_000

TLJH_R2D_ADMIN_SCOPE = "custom:tljh_repo2docker:admin"


class SpawnerMixin(Configurable):
    """
    Mixin for spawners that derive from DockerSpawner, to use local Docker images
    built with tljh-repo2docker.

    Call `set_limits` in the spawner `start` method to set the memory and cpu limits.
    """

    image_form_template = Unicode(
        """
        <style>
            #image-list {
                max-height: 600px;
                overflow: auto;
            }
            .image-info {
                font-weight: normal;
            }
        </style>
        <script>
            setTimeout(function() {
                selectServers();
            }, 100);

            function selectServers() {
                if (!window.location.hash.match(/^#.+/)) {
                    return;
                }
                const ref = window.location.hash.match(/^#(.+)$/)[1];
                $("input[image-data='" + ref + "']").prop('checked', true);
                console.log(ref);
            }
        </script>
        <div class='form-group' id='image-list'>
        {% for image in image_list %}
        <label for='image-item-{{ loop.index0 }}' class='form-control input-group'>
            <div class='col-md-1'>
                <input type='radio' name='image' image-data='{{ image.spawnref }}' id='image-item-{{ loop.index0 }}' value='{{ image.image_name }}' />
            </div>
            <div class='col-md-11'>
                <strong>{{ image.display_name }}</strong>
                <div class='row image-info'>
                    <div class='col-md-4'>
                        Repository:
                    </div>
                    <div class='col-md-8'>
                        <a href="{{ image.repo }}" target="_blank">{{ image.repo }}</a>
                    </div>
                </div>
                <div class='row image-info'>
                    <div class='col-md-4'>
                        Memory Limit (GB):
                    </div>
                    <div class='col-md-8'>
                        <strong>{{ image.mem_limit | replace("G", "") }}</strong>
                    </div>
                </div>
                <div class='row image-info'>
                    <div class='col-md-4'>
                        CPU Limit:
                    </div>
                    <div class='col-md-8'>
                        <strong>{{ image.cpu_limit }}</strong>
                    </div>
                </div>
            </div>
        </label>
        {% endfor %}
        </div>
        """,
        config=True,
        help="""
        Jinja2 template for constructing the list of images shown to the user.
        """,
    )

    rdmfs_base_path = Unicode(
        config=True,
        help="""
        A base path for RDMFS.
        """,
    )

    token_store_path = Unicode(
        config=True,
        help="""
        A dbpath of token_store.
        """,
    )

    extra_mounts = None

    async def list_images(self):
        """
        Return the list of available images
        """
        return await list_images()

    async def get_options_form(self):
        """
        Override the default form to handle the case when there is only one image.
        """
        try:
            images = await self.list_images()
        except ValueError:
            images = []

        # make default limits human readable
        default_mem_limit = self.mem_limit
        if isinstance(default_mem_limit, (float, int)):
            # default memory unit is in GB
            default_mem_limit /= ByteSpecification.UNIT_SUFFIXES["G"]
            if float(default_mem_limit).is_integer():
                default_mem_limit = int(default_mem_limit)

        default_cpu_limit = self.cpu_limit
        if default_cpu_limit and float(default_cpu_limit).is_integer():
            default_cpu_limit = int(default_cpu_limit)

        # add memory and cpu limits
        for image in images:
            image["mem_limit"] = image["mem_limit"] or default_mem_limit
            image["cpu_limit"] = image["cpu_limit"] or default_cpu_limit

        image_form_template = Environment(loader=BaseLoader).from_string(
            self.image_form_template
        )
        return image_form_template.render(image_list=images)

    async def set_limits(self):
        """
        Set the user environment limits if they are defined in the image
        """
        imagename = self.user_options.get("image")
        async with Docker() as docker:
            image = await docker.images.inspect(imagename)
        config = image.get("ContainerConfig", None)
        if not config:
            config = image.get("Config", {})
        label = config.get("Labels", {})
        mem_limit = label.get("tljh_repo2docker.mem_limit", None)
        cpu_limit = label.get("tljh_repo2docker.cpu_limit", None)

        # override the spawner limits if defined in the image
        if mem_limit:
            self.mem_limit = mem_limit
        if cpu_limit:
            self.cpu_limit = float(cpu_limit)

        if self.cpu_limit:
            self.extra_host_config.update(
                {
                    "cpu_period": CPU_PERIOD,
                    "cpu_quota": int(float(CPU_PERIOD) * self.cpu_limit),
                }
            )

    async def set_extra_mounts(self):
        """
        Prepare volume binds for GRDM
        """
        imagename = self.user_options.get("image")
        async with Docker() as docker:
            image = await docker.images.inspect(imagename)
        
        labels = self._get_image_labels(image)
        provider_prefix = labels.get(
            "tljh_repo2docker.opt.provider", None
        )
        if provider_prefix != 'rdm':
            return
        await self._set_rdm_mounts(image)

    async def _set_rdm_mounts(self, image):
        labels = self._get_image_labels(image)
        provider_prefix = labels.get(
            "tljh_repo2docker.opt.provider", None
        )
        if provider_prefix != 'rdm':
            return
        repo = labels.get(
            "tljh_repo2docker.opt.repo", None
        )
        token_store = TokenStore(dbpath=self.token_store_path)
        repo_token = token_store.get(self.user, repo)
        if repo_token is None:
            raise web.HTTPError(
                400,
                "No repo_token for: %s" % (repo),
            )
        self.log.info("Preparing RDMFS... " + 'name=' + repr(self.user.name) + ', repo=' + repr(repo))
        mount_path = os.path.join(self.rdmfs_base_path, self.container_name)
        if not os.path.exists(mount_path):
            os.makedirs(mount_path)
        self.extra_mounts = [
            dict(type='bind', source=mount_path, target='/mnt', propagation='rshared'),
        ]
        rdmfs_id = await self.get_rdmfs_object()
        if rdmfs_id is not None:
            await self.remove_object_by_id(rdmfs_id)
        rdmfs_id = await self.create_rdmfs_object({
            'RDM_NODE_ID': labels.get(
                "tljh_repo2docker.opt.user.rdm_node_id", None
            ),
            'RDM_API_URL': labels.get(
                "tljh_repo2docker.opt.user.rdm_api_url", None
            ),
            'RDM_TOKEN': repo_token,
            'MOUNT_PATH': '/mnt/rdm',
        })
        await self.start_object_by_id(rdmfs_id)

    async def get_rdmfs_object(self):
        object_name = self.object_name + '_rdmfs'
        self.log.debug("Getting %s '%s'", self.object_type, object_name)
        try:
            async with Docker() as docker:
                obj = await docker.containers.get(object_name)
            return obj.id
        except DockerError as e:
            if e.status == 404:
                self.log.info(
                    "%s '%s' is gone", self.object_type.title(), object_name
                )
            elif e.status == 500:
                self.log.info(
                    "%s '%s' is on unhealthy node",
                    self.object_type.title(),
                    object_name,
                )
            else:
                raise
        return None

    async def create_rdmfs_object(self, env):
        host_config = dict(
            Mounts=[
                {
                    "Type": "bind",
                    "Source": m['source'],
                    "Target": "/mnt",
                    "ReadOnly": False,
                    "BindOptions": {
                        "Propagation": "rshared",
                    },
                }
                for m in (self.extra_mounts or [])
            ],
            Privileged=True,
        )
        create_kwargs = dict(
            Image='gcr.io/nii-ap-ops/rdmfs:2024.12.0',
            Env=[f'{k}={v}' for k, v in env.items()],
            AutoRemove=True,
            HostConfig=host_config,
        )
        async with Docker() as docker:
            obj = await docker.containers.create(
                create_kwargs,
                name=self.container_name + '_rdmfs',
            )
        return obj.id

    async def start_object_by_id(self, object_id):
        async with Docker() as docker:
            obj = await docker.containers.get(object_id)
            await obj.start()

    async def remove_object_by_id(self, object_id):
        self.log.info("Removing %s %s", self.object_type, object_id)
        try:
            async with Docker() as docker:
                obj = await docker.containers.get(object_id)
                desc = await obj.show()
                if 'State' in desc and desc['State']['Running']:
                    self.log.info('terminating...')
                    exec = await obj.exec(["/bin/sh","-c","xattr -w command terminate /mnt/rdm"])
                    result = await exec.start(detach=True)
                    self.log.info('terminated: {}'.format(result))
                else:
                    self.log.info('deleting...')
                    await obj.delete()
        except DockerError as e:
            if e.status == 409:
                self.log.debug(
                    "Already removing %s: %s", self.object_type, object_id
                )
            elif e.status == 404:
                self.log.debug(
                    "Already removed %s: %s", self.object_type, object_id
                )
            else:
                raise

    def _get_image_labels(self, image):
        config = image.get("ContainerConfig", None)
        if not config:
            config = image.get("Config", {})
        return config.get("Labels", {})


class Repo2DockerSpawner(SpawnerMixin, DockerSpawner):
    """
    A custom spawner for using local Docker images built with tljh-repo2docker.
    """

    @property
    def mount_binds(self):
        base_mount_binds = super().mount_binds.copy()
        if self.extra_mounts is None:
            return base_mount_binds
        base_mount_binds += [Mount(**m) for m in self.extra_mounts]
        return base_mount_binds

    async def start(self, *args, **kwargs):
        await self.set_limits()
        await self.set_extra_mounts()
        return await super().start(*args, **kwargs)

    async def stop(self, *args, **kwargs):
        await super().stop(*args, **kwargs)
        rdmfs_id = await self.get_rdmfs_object()
        if rdmfs_id is None:
            return
        await self.remove_object_by_id(rdmfs_id)


if hookimpl:

    @hookimpl
    def tljh_custom_jupyterhub_config(c):
        from .custom_providers import token_store_path

        # hub
        c.JupyterHub.hub_ip = public_ips()[0]
        c.JupyterHub.cleanup_servers = False
        c.JupyterHub.spawner_class = Repo2DockerSpawner

        # spawner
        c.DockerSpawner.cmd = ["jupyterhub-singleuser"]
        c.DockerSpawner.pull_policy = "Never"
        c.DockerSpawner.remove = True
        c.Repo2DockerSpawner.rdmfs_base_path = '/opt/tljh/repo2docker/volumes'
        c.Repo2DockerSpawner.token_store_path = token_store_path

        # Launch Handler for backward compatibility
        # Since c.JupyterHub.extra_handlers is deprecated,
        # please specify http://hostname/services/tljh-repo2docker instead of http://hostname
        # as a BinderHub URL when registering to GakuNin RDM.
        c.JupyterHub.extra_handlers.extend(
            [
                (
                    r"build/([^/]+)/[^/]+/[^/]+",
                    LaunchRedirectHandler,
                ),
            ]
        )

    @hookimpl
    def tljh_extra_hub_pip_packages():
        return [
            "dockerspawner~=12.1",
            "jupyter_client~=6.1,<8",
            "aiodocker~=0.19",
            "git+https://github.com/RCOSDP/CS-binderhub.git@master",
        ]

else:
    tljh_custom_jupyterhub_config = None
    tljh_extra_hub_pip_packages = None
