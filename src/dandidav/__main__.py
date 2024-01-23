from __future__ import annotations
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
import io
from operator import attrgetter
import os
import re
from typing import IO, TYPE_CHECKING
from urllib.parse import quote
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from cheroot import wsgi
from dandi.consts import DANDISET_ID_REGEX, PUBLISHED_VERSION_REGEX
from dandi.dandiapi import (
    DandiAPIClient,
    RemoteAsset,
    RemoteBlobAsset,
    RemoteDandiset,
    RemoteZarrAsset,
)
from dandi.exceptions import NotFoundError
import fsspec
from ruamel.yaml import YAML
from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider
from wsgidav.util import join_uri
from wsgidav.wsgidav_app import WsgiDAVApp

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

INSTANCE = "dandi"
TOKEN: str | None = None
BUCKET = "dandiarchive"

# If a client makes a request for a path with one of these names, assume it
# doesn't exist without checking the Archive:
FAST_NOT_EXIST = [".git", ".svn", ".bzr", ".nols"]


class DandiProvider(DAVProvider):
    def __init__(self) -> None:
        super().__init__()
        self.client = DandiAPIClient.for_dandi_instance(INSTANCE, token=TOKEN)

    def get_resource_inst(
        self, path: str, environ: dict
    ) -> DAVCollection | DAVNonCollection:
        return RootCollection("/", environ).resolve("/", path)

    def is_readonly(self) -> bool:
        return True


class RootCollection(DAVCollection):
    def get_member_names(self) -> list[str]:
        return ["dandisets"]

    def get_member(self, name: str) -> DandisetCollection | None:
        if name == "dandisets":
            return DandisetCollection(join_uri(self.path, name), self.environ)
        else:
            return None

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False


class DandisetCollection(DAVCollection):
    """Collection of Dandisets in instance"""

    def __init__(self, path: str, environ: dict) -> None:
        super().__init__(path, environ)
        self.client: DandiAPIClient = self.provider.client

    def get_member_list(self) -> list[DandisetResource]:
        return [
            DandisetResource(join_uri(self.path, d.identifier), self.environ, d)
            for d in self.client.get_dandisets()
        ]

    def get_member_names(self) -> list[str]:
        return [d.identifier for d in self.client.get_dandisets()]

    def get_member(self, name: str) -> DandisetResource | None:
        if not re.fullmatch(DANDISET_ID_REGEX, name):
            return None
        try:
            d = self.client.get_dandiset(name, lazy=False)
        except NotFoundError:
            return None
        else:
            return DandisetResource(join_uri(self.path, name), self.environ, d)

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False


class DandisetResource(DAVCollection):
    def __init__(self, path: str, environ: dict, dandiset: RemoteDandiset) -> None:
        super().__init__(path, environ)
        self.dandiset = dandiset

    def get_display_info(self) -> dict[str, str]:
        return {"type": "Dandiset"}

    def get_member_names(self) -> list[str]:
        names = ["draft"]
        if self.dandiset.most_recent_published_version is not None:
            names.append("latest")
            names.append("releases")
        return names

    def get_member(self, name: str) -> VersionResource | ReleasesCollection | None:
        if name == "draft":
            d = self.dandiset.for_version(self.dandiset.draft_version)
            return VersionResource(join_uri(self.path, name), self.environ, d)
        elif (
            name == "latest"
            and (v := self.dandiset.most_recent_published_version) is not None
        ):
            d = self.dandiset.for_version(v)
            return VersionResource(join_uri(self.path, name), self.environ, d)
        elif (
            name == "releases"
            and self.dandiset.most_recent_published_version is not None
        ):
            return ReleasesCollection(
                join_uri(self.path, name), self.environ, self.dandiset
            )
        else:
            return None

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def get_creation_date(self) -> float:
        dt = self.dandiset.created
        assert isinstance(dt, datetime)
        return dt.timestamp()

    def get_last_modified(self) -> float:
        dt = self.dandiset.modified
        assert isinstance(dt, datetime)
        return dt.timestamp()


class ReleasesCollection(DAVCollection):
    def __init__(self, path: str, environ: dict, dandiset: RemoteDandiset) -> None:
        super().__init__(path, environ)
        self.dandiset = dandiset

    def get_display_info(self) -> dict[str, str]:
        return {"type": "Dandiset releases"}

    def get_member_list(self) -> list[VersionResource]:
        return [
            VersionResource(
                join_uri(self.path, v.identifier),
                self.environ,
                self.dandiset.for_version(v),
            )
            for v in self.dandiset.get_versions()
            if v.identifier != "draft"
        ]

    def get_member_names(self) -> list[str]:
        return [
            v.identifier
            for v in self.dandiset.get_versions()
            if v.identifier != "draft"
        ]

    def get_member(self, name: str) -> VersionResource | None:
        if not re.fullmatch(PUBLISHED_VERSION_REGEX, name):
            return None
        try:
            d = self.dandiset.for_version(name)
        except NotFoundError:
            return None
        else:
            return VersionResource(join_uri(self.path, name), self.environ, d)

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False


class AssetFolder(DAVCollection):
    def __init__(
        self, path: str, environ: dict, dandiset: RemoteDandiset, asset_path_prefix: str
    ) -> None:
        super().__init__(path, environ)
        self.dandiset = dandiset
        self.asset_path_prefix = asset_path_prefix

    def get_member_list(self) -> list[BlobResource | ZarrResource | AssetFolder]:
        members = []
        for n in self.iter_dandi_folder():
            if isinstance(n, DandiAssetFolder):
                members.append(
                    AssetFolder(
                        join_uri(self.path, n.name),
                        self.environ,
                        self.dandiset,
                        n.prefix,
                    )
                )
            else:
                assert isinstance(n, DandiAsset)
                asset = self.dandiset.get_asset(n.asset_id)
                members.append(self.make_asset_resource(n.name, asset))
        return members

    def get_member_names(self) -> list[str]:
        return [n.name for n in self.iter_dandi_folder()]

    def get_member(self, name: str) -> BlobResource | ZarrResource | AssetFolder | None:
        if name in FAST_NOT_EXIST:
            return None
        if self.asset_path_prefix == "":
            prefix = name
        else:
            prefix = f"{self.asset_path_prefix}/{name}"
        for a in self.dandiset.get_assets_with_path_prefix(prefix, order="path"):
            if a.path == prefix:
                return self.make_asset_resource(name, a)
            elif a.path.startswith(f"{prefix}/"):
                return AssetFolder(
                    join_uri(self.path, name),
                    self.environ,
                    self.dandiset,
                    prefix,
                )
        return None

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def make_asset_resource(
        self, name: str, asset: RemoteAsset
    ) -> BlobResource | ZarrResource:
        if isinstance(asset, RemoteBlobAsset):
            return BlobResource(join_uri(self.path, name), self.environ, asset)
        else:
            assert isinstance(asset, RemoteZarrAsset)
            return ZarrResource(join_uri(self.path, name), self.environ, asset)

    def iter_dandi_folder(self) -> Iterator[DandiAssetFolder | DandiAsset]:
        path = (
            f"/dandisets/{self.dandiset.identifier}/versions"
            f"/{self.dandiset.version.identifier}/assets/paths"
        )
        for node in self.dandiset.client.paginate(
            path, params={"path_prefix": self.asset_path_prefix}
        ):
            if self.asset_path_prefix == "":
                name = node["path"]
            else:
                name = node["path"].removeprefix(f"{self.asset_path_prefix}/")
            if node["asset"] is not None:
                yield DandiAsset(name, asset_id=node["asset"]["asset_id"])
            else:
                yield DandiAssetFolder(name, prefix=node["path"])


@dataclass
class DandiAsset:
    name: str
    asset_id: str


@dataclass
class DandiAssetFolder:
    name: str
    prefix: str


class BlobResource(DAVNonCollection):
    def __init__(self, path: str, environ: dict, asset: RemoteBlobAsset) -> None:
        super().__init__(path, environ)
        self.asset = asset

    def get_content(self) -> IO[bytes]:
        return self.asset.as_readable().open()  # type: ignore[no-any-return]

    def support_ranges(self) -> bool:
        return True

    def get_content_length(self) -> int:
        s = self.asset.size
        assert isinstance(s, int)
        return s

    def get_content_type(self) -> str:
        try:
            ct = self.asset.get_raw_metadata()["encodingFormat"]
        except KeyError:
            return "application/octet-stream"
        else:
            assert isinstance(ct, str)
            return ct

    def get_display_info(self) -> dict:
        return {"type": "Blob asset"}

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def get_etag(self) -> str | None:
        try:
            dg = self.asset.get_raw_digest()
        except NotFoundError:
            return None
        else:
            assert dg is None or isinstance(dg, str)
            return dg

    def support_etag(self) -> bool:
        return True

    def get_creation_date(self) -> float:
        dt = self.asset.created
        assert isinstance(dt, datetime)
        return dt.timestamp()

    def get_last_modified(self) -> float:
        dt = self.asset.modified
        assert isinstance(dt, datetime)
        return dt.timestamp()


class VersionResource(AssetFolder):
    """
    A Dandiset at a specific version, containing top-level assets and asset
    folders
    """

    def __init__(self, path: str, environ: dict, dandiset: RemoteDandiset) -> None:
        super().__init__(path, environ, dandiset, "")

    def get_display_info(self) -> dict[str, str]:
        return {"type": "Dandiset version"}

    def get_member_list(
        self,
    ) -> list[BlobResource | ZarrResource | AssetFolder | DandisetYaml]:
        members = super().get_member_list()
        members.append(
            DandisetYaml(
                join_uri(self.path, "dandiset.yaml"), self.environ, self.dandiset
            )
        )
        members.sort(key=attrgetter("name"))
        return members

    def get_member_names(self) -> list[str]:
        names = super().get_member_names()
        names.append("dandiset.yaml")
        names.sort()
        return names

    def get_member(
        self, name: str
    ) -> BlobResource | ZarrResource | AssetFolder | DandisetYaml | None:
        if name == "dandiset.yaml":
            return DandisetYaml(
                join_uri(self.path, "dandiset.yaml"), self.environ, self.dandiset
            )
        else:
            return super().get_member(name)

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def get_creation_date(self) -> float:
        dt = self.dandiset.version.created
        assert isinstance(dt, datetime)
        return dt.timestamp()

    def get_last_modified(self) -> float:
        dt = self.dandiset.version.modified
        assert isinstance(dt, datetime)
        return dt.timestamp()


class DandisetYaml(DAVNonCollection):
    def __init__(self, path: str, environ: dict, dandiset: RemoteDandiset) -> None:
        super().__init__(path, environ)
        self.dandiset = dandiset

    def get_content(self) -> IO[bytes]:
        yaml = YAML(typ="safe")
        yaml.default_flow_style = False
        out = io.BytesIO()
        yaml.dump(self.dandiset.get_raw_metadata(), out)
        out.seek(0)
        return out

    def support_ranges(self) -> bool:
        return True

    def get_content_length(self) -> int:
        fp = self.get_content()
        fp.seek(0, os.SEEK_END)
        return fp.tell()

    def get_content_type(self) -> str:
        return "text/yaml; charset=utf-8"

    def get_display_info(self) -> dict:
        return {"type": "Dandiset metadata"}

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def get_etag(self) -> None:
        return None

    def support_etag(self) -> bool:
        return False


class ZarrFolder(DAVCollection):
    def __init__(
        self, path: str, environ: dict, s3client: S3Client, prefix: str
    ) -> None:
        super().__init__(path, environ)
        self.s3client = s3client
        self.prefix = prefix

    def get_member_list(self) -> list[ZarrFolder | ZarrEntryResource]:
        members = []
        for n in self.iter_zarr_folder():
            if isinstance(n, S3Folder):
                members.append(
                    ZarrFolder(
                        join_uri(self.path, n.name),
                        self.environ,
                        self.s3client,
                        self.prefix + n.name + "/",
                    )
                )
            else:
                assert isinstance(n, S3Entry)
                members.append(
                    ZarrEntryResource(join_uri(self.path, n.name), self.environ, n)
                )
        return members

    def get_member_names(self) -> list[str]:
        return [n.name for n in self.iter_zarr_folder()]

    def get_member(self, name: str) -> ZarrFolder | ZarrEntryResource | None:
        if name in FAST_NOT_EXIST:
            return None
        prefix = self.prefix + name
        for page in self.s3client.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/"
        ):
            for n in page.get("Contents", []):
                if n["Key"] == prefix:
                    data = S3Entry(
                        name=name,
                        size=n["Size"],
                        modified=n["LastModified"],
                        etag=n["ETag"].strip('"'),
                        url=f"https://{BUCKET}.s3.amazonaws.com/{quote(n['Key'])}",
                    )
                    return ZarrEntryResource(
                        join_uri(self.path, name), self.environ, data
                    )
            for prefx in page.get("CommonPrefixes", []):
                if prefx["Prefix"] == prefix + "/":
                    return ZarrFolder(
                        join_uri(self.path, name),
                        self.environ,
                        self.s3client,
                        self.prefix + name + "/",
                    )
        return None

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def iter_zarr_folder(self) -> Iterator[S3Folder | S3Entry]:
        for page in self.s3client.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=self.prefix, Delimiter="/"
        ):
            for prefx in page.get("CommonPrefixes", []):
                yield S3Folder(name=prefx["Prefix"].removeprefix(self.prefix))
            for n in page.get("Contents", []):
                yield S3Entry(
                    name=n["Key"].removeprefix(self.prefix),
                    size=n["Size"],
                    modified=n["LastModified"],
                    etag=n["ETag"].strip('"'),
                    url=f"https://{BUCKET}.s3.amazonaws.com/{quote(n['Key'])}",
                )


@dataclass
class S3Folder:
    name: str


@dataclass
class S3Entry:
    name: str
    size: int
    modified: datetime
    etag: str
    url: str


class ZarrEntryResource(DAVNonCollection):
    def __init__(self, path: str, environ: dict, data: S3Entry) -> None:
        super().__init__(path, environ)
        self.data = data

    def get_content(self) -> IO[bytes]:
        return fsspec.open(self.data.url, mode="rb").open()  # type: ignore[no-any-return]

    def support_ranges(self) -> bool:
        return True

    def get_content_length(self) -> int:
        return self.data.size

    def get_content_type(self) -> str:
        return "application/octet-stream"

    def get_display_info(self) -> dict:
        return {"type": "Zarr entry"}

    def is_link(self) -> bool:
        # Fix for <https://github.com/mar10/wsgidav/issues/301>
        return False

    def get_etag(self) -> str:
        return self.data.etag

    def support_etag(self) -> bool:
        return True

    def get_last_modified(self) -> float:
        dt = self.data.modified
        assert isinstance(dt, datetime)
        return dt.timestamp()


class ZarrResource(ZarrFolder):
    def __init__(self, path: str, environ: dict, asset: RemoteZarrAsset) -> None:
        s3client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        prefix = f"zarr/{asset.zarr}/"
        super().__init__(path, environ, s3client, prefix)
        self.asset = asset

    def get_display_info(self) -> dict:
        return {"type": "Zarr asset"}

    def get_creation_date(self) -> float:
        dt = self.asset.created
        assert isinstance(dt, datetime)
        return dt.timestamp()

    def get_last_modified(self) -> float:
        dt = self.asset.modified
        assert isinstance(dt, datetime)
        return dt.timestamp()


def main() -> None:
    config = {
        "host": "127.0.0.1",
        "port": 8080,
        "provider_mapping": {
            "/": DandiProvider(),
        },
        "simple_dc": {
            "user_mapping": {
                "/": True,
            },
        },
        "verbose": 4,
    }
    app = WsgiDAVApp(config)
    server = wsgi.Server(
        bind_addr=(config["host"], config["port"]),
        wsgi_app=app,
    )
    try:
        server.start()
    except KeyboardInterrupt:
        print("Received Ctrl-C: stopping...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
