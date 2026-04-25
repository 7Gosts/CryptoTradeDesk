#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书私聊文本发送（open_id）。

独立可复用模块：不依赖项目内其它业务模块（仅依赖 requests）。
鉴权仅通过：
  - 参数：app_id / app_secret
  - 或环境变量：FEISHU_APP_ID / FEISHU_APP_SECRET
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class FeishuCredential:
    app_id: str
    app_secret: str


def load_credential(app_id: str | None = None, app_secret: str | None = None) -> FeishuCredential:
    a = (app_id or os.environ.get("FEISHU_APP_ID") or "").strip()
    s = (app_secret or os.environ.get("FEISHU_APP_SECRET") or "").strip()
    if not a or not s:
        raise RuntimeError("缺少飞书凭据：请设置 FEISHU_APP_ID / FEISHU_APP_SECRET，或通过参数传入。")
    return FeishuCredential(app_id=a, app_secret=s)


def get_tenant_access_token(cred: FeishuCredential) -> str:
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        headers={"Content-Type": "application/json; charset=utf-8"},
        json={"app_id": cred.app_id, "app_secret": cred.app_secret},
        timeout=(10, 30),
    ).json()
    token = resp.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"获取 tenant_access_token 失败: {resp}")
    return str(token)


def send_text(open_id: str, text: str, tenant_access_token: str) -> dict[str, Any]:
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    resp = requests.post(
        url,
        params={"receive_id_type": "open_id"},
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {tenant_access_token}",
        },
        json={
            "receive_id": open_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        },
        timeout=(10, 30),
    ).json()
    return resp

