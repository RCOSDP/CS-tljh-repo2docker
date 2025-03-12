# CS-tljh-repo2docker

![Github Actions Status](https://github.com/plasmabio/tljh-repo2docker/workflows/Tests/badge.svg)

CS-tljh-repo2docker プラグインは、ユーザー環境にインストール可能な、[データ解析機能相当のサービス](https://support.rdm.nii.ac.jp/usermanual/DataAnalysis-01/) です。

## インストール手順

[TLJHのインストールプロセス](https://tljh.jupyter.org/en/latest/install/index.html) において、以下のスクリプトを実行してください。

`--admin` 引数中の `change-your-password` は適切なパスワードに変更して実行してください。

```bash
#!/bin/bash

# install Docker
sudo apt update && sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable"
sudo apt update && sudo apt install -y docker-ce

sudo apt-get update
curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
sudo chmod a+r /etc/apt/keyrings/nodesource.gpg

NODE_MAJOR=21
echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | \
 sudo tee /etc/apt/sources.list.d/nodesource.list
sudo apt update
sudo apt install -y nodejs
sudo npm install -g yarn

sudo modprobe fuse

# pull the repo2docker image
sudo docker pull gcr.io/nii-ap-ops/repo2docker:2025.01.0
sudo docker pull gcr.io/nii-ap-ops/rdmfs:2024.12.0

# install TLJH 1.0
curl -L https://tljh.jupyter.org/bootstrap.py \
  | sudo python3 - \
    --version 1.0.0 \
    --admin admin:change-your-password \
    --plugin git+https://github.com/RCOSDP/CS-tljh-repo2docker.git@master

# Workaround: upgrade to the latest version of jupyterhub
# Because an older version of jupyterhub is installed together with CS-binderhub,
# upgrade jupyterhub after the installation of the plugin
sudo /opt/tljh/hub/bin/pip install --upgrade jupyterhub\<5

# configure the plugin
cat <<'EOF' | sudo tee /opt/tljh/config/jupyterhub_config.d/repo2docker.py
from tljh_repo2docker import TLJH_R2D_ADMIN_SCOPE
import sys


c.JupyterHub.allow_named_servers = True

c.JupyterHub.services.extend(
    [
        {
            "name": "tljh_repo2docker",
            "url": "http://127.0.0.1:6789", # URL must match the `ip` and `port` config
            "command": [
                sys.executable,
                "-m",
                "tljh_repo2docker",
                "--ip",
                "127.0.0.1",
                "--port",
                "6789"
            ],
            "oauth_no_confirm": True,
            "oauth_client_allowed_scopes": [
                TLJH_R2D_ADMIN_SCOPE, # Allows this service to check if users have its admin scope.
            ],
        }
    ]
)

c.JupyterHub.custom_scopes = {
    TLJH_R2D_ADMIN_SCOPE: {
        "description": "Admin access to tljh_repo2docker",
    },
}

# Set required scopes for the service and users
c.JupyterHub.load_roles = [
    {
        "description": "Role for tljh_repo2docker service",
        "name": "tljh-repo2docker-service",
        "scopes": [
            "read:users",
            "read:roles:users",
            "admin:servers",
            "access:services!service=binder",
        ],
        "services": ["tljh_repo2docker"],
    },
    {
        "name": "user",
        "scopes": [
            "self",
            # access to the serve page
            "access:services!service=tljh_repo2docker",
        ],
    },
    {
        "name": 'tljh-repo2docker-service-admin',
        "groups": ["repo2docker"],
        "scopes": [TLJH_R2D_ADMIN_SCOPE],
    },
]

c.JupyterHub.tornado_settings = {
    "slow_spawn_timeout": 30
}
EOF
sudo systemctl restart jupyterhub
```

上記の設定を適用すると、JupyterHubの管理者または `repo2docker` グループのユーザーは `tljh_repo2docker` サービスにアクセスできるようになります。設定のカスタマイズ方法については、[Configuration](README.md#configuration)を参照してください。

TLJHプラグインのインストールに関する詳細は、[The Littlest JupyterHub documentation](https://tljh.jupyter.org/en/latest/topic/customizing-installer.html?highlight=plugins#installing-tljh-plugins)を参照してください。

## 使用方法

[GakuNin RDMユーザーマニュアル 外部解析基盤との連携](https://support.rdm.nii.ac.jp/usermanual/DataAnalysis-06/)を参照してください。
