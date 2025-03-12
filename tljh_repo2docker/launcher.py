from datetime import datetime
import json
import os
import re

from traitlets.config import Config
from .base import BaseHandler, require_admin_role
from aiodocker import Docker, DockerError
from tornado import web
from tornado.httpclient import AsyncHTTPClient

from .docker import build_image
from .token import TokenStore


class LaunchHandler(BaseHandler):
    """
    Handle requests from GRDM to build user environments as Docker images
    """
    def initialize(self, repo_providers, token_store_path):
        self.log = self.settings['log']
        self.service_prefix = self.settings['service_prefix']
        self.repo_providers = repo_providers
        self.token_store = TokenStore(dbpath=token_store_path)

    @web.authenticated
    @require_admin_role
    async def get(self, provider_prefix):
        from binderhub.builder import _safe_build_slug, _generate_build_name

        current_user = await self.fetch_user()
        spec = self._get_spec_from_request(provider_prefix)
        spec = spec.rstrip("/")
        provider = self._get_provider(provider_prefix, spec)
        repo = provider.get_repo_url()
        ref = await provider.get_resolved_ref()

        image_prefix = ''
        safe_build_slug = _safe_build_slug(provider.get_build_slug(), limit=255 - len(image_prefix))
        build_name = _generate_build_name(provider.get_build_slug(), ref, prefix='build-')
        image_name = self.image_name = '{prefix}{build_slug}:{ref}'.format(
            prefix=image_prefix,
            build_slug=safe_build_slug,
            ref=ref
        ).replace('_', '-').lower()
        memory = None
        cpu = None
        buildargs = None
        username = None
        password = None
        repo_token = self.get_argument('repo_token', None)
        urlpath = self.get_argument('urlpath', None)
        if not repo:
            raise web.HTTPError(400, "Repository is empty")
        self.token_store.set(current_user, repo, repo_token)

        optional_labels = {
            'provider': provider_prefix,
            'repo': repo,
            'ref': ref,
            'builder': current_user.name,
            'urlpath': urlpath,
        }
        for key, values in self.request.query_arguments.items():
            if not key.startswith('useropt.'):
                continue
            self.log.info(f"extra_args: {key}={values}")
            optional_labels["user." + key[8:]] = "\t".join([v.decode("utf8") for v in values])
        optional_labels = await self._modify_labels(optional_labels, repo_token)

        await build_image(
            repo, ref, '', memory, cpu, username, password, [],
            default_image_name=image_name,
            repo2docker_image='gcr.io/nii-ap-ops/repo2docker:2025.01.0',
            optional_envs=provider.get_optional_envs(access_token=repo_token),
            optional_labels=optional_labels,
        )

        self.redirect(f'{self.service_prefix}environments')

    def _get_provider(self, provider_prefix, spec):
        """Construct a provider object"""
        if provider_prefix not in self.repo_providers:
            raise web.HTTPError(404, "No provider found for prefix %s" % provider_prefix)

        c = Config()
        rdm_provider_hosts = [
            {
                'hostname': ["https://osf.io/"],
                'api': "https://api.osf.io/v2/"
            },
            {
                'hostname': ["https://rdm.nii.ac.jp"],
                'api': "https://api.rdm.nii.ac.jp/v2/",
            },
            {
                'hostname': ["https://rcos.rdm.nii.ac.jp"],
                'api': "https://api.rcos.rdm.nii.ac.jp/v2/",
            },
        ]
        custom_hosts = os.environ.get('REPO2DOCKER_RDM_PROVIDER_HOSTS', None)
        if custom_hosts is not None:
            rdm_provider_hosts = json.loads(custom_hosts)
        c.RDMProvider.hosts = rdm_provider_hosts
        return self.repo_providers[provider_prefix](
            config=c, spec=spec)

    def _get_spec_from_request(self, prefix):
        """Re-extract spec from request.path.
        Get the original, raw spec, without tornado's unquoting.
        This is needed because tornado converts 'foo%2Fbar/ref' to 'foo/bar/ref'.
        """
        idx = self.request.path.index(prefix)
        spec = self.request.path[idx + len(prefix) + 1:]
        return spec

    async def _modify_labels(self, labels, repo_token):
        if 'provider' not in labels:
            return labels
        if labels['provider'] != 'rdm':
            return labels
        node_api_url = labels['user.rdm_node']
        http_client = AsyncHTTPClient()
        try:
            headers = {
                'Authorization': f'Bearer {repo_token}',
            }
            response = await http_client.fetch(node_api_url, headers=headers)
            node_data = json.loads(response.body)
            node_title = node_data['data']['attributes']['title']
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            labels['provider.display_name'] = f'{node_title} - {now}'
            labels['provider.repo'] = node_data['data']['links']['html']
            labels['provider.ref_url'] = labels['provider.repo']
        except Exception as e:
            self.log.exception(f'Cannot retrieve GRDM repository: {node_api_url}')
            raise web.HTTPError(500, "Cannot retrieve GRDM repository")
        return labels
