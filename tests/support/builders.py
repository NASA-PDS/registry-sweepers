from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

from tests.support.documents import SeedDoc


@dataclass
class ProductDocumentBuilder:
    lidvid: str
    product_class: str
    title: str = "Test Product"
    ops_ancestry_version: Optional[int] = None
    alternate_ids: List[str] = field(default_factory=list)
    ref_lid_collection: List[str] = field(default_factory=list)
    ref_lidvid_collection: List[str] = field(default_factory=list)

    def with_ancestry_version(self, version: int) -> "ProductDocumentBuilder":
        self.ops_ancestry_version = version
        return self

    def with_alternate_id(self, alt_id: str) -> "ProductDocumentBuilder":
        self.alternate_ids.append(alt_id)
        return self

    def with_title(self, title: str) -> "ProductDocumentBuilder":
        self.title = title
        return self

    def with_collection_lid_reference(self, collection_lid: str) -> "ProductDocumentBuilder":
        self.ref_lid_collection.append(collection_lid)
        return self

    def with_collection_lidvid_reference(self, collection_lidvid: str) -> "ProductDocumentBuilder":
        self.ref_lidvid_collection.append(collection_lidvid)
        return self

    def build(self) -> Dict:
        source: Dict = {
            "lidvid": self.lidvid,
            "product_class": self.product_class,
            "title": self.title,
            "alternate_ids": self.alternate_ids,
        }

        if self.ops_ancestry_version is not None:
            source[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = self.ops_ancestry_version

        if self.ref_lid_collection:
            source["ref_lid_collection"] = self.ref_lid_collection

        if self.ref_lidvid_collection:
            source["ref_lidvid_collection"] = self.ref_lidvid_collection

        return {"_source": source, "_id": self.lidvid}

    def to_seed(self, index: str = "registry") -> SeedDoc:
        doc = self.build()
        return SeedDoc(index=index, id=doc["_id"], source=doc["_source"])


@dataclass
class CollectionRefsBuilder:
    collection_lidvid: str
    product_lidvids: List[str] = field(default_factory=list)
    batch_id: int = 1

    def with_product(self, lidvid: str) -> "CollectionRefsBuilder":
        self.product_lidvids.append(lidvid)
        return self

    def with_products(self, lidvids: List[str]) -> "CollectionRefsBuilder":
        self.product_lidvids.extend(lidvids)
        return self

    def with_batch_id(self, batch_id: int) -> "CollectionRefsBuilder":
        self.batch_id = batch_id
        return self

    def build(self) -> Dict:
        return {
            "_id": f"{self.collection_lidvid}::batch_{self.batch_id}",
            "_source": {
                "collection_lidvid": self.collection_lidvid,
                "batch_id": self.batch_id,
                "product_lidvid": self.product_lidvids,
            },
        }

    def to_seed(self) -> SeedDoc:
        doc = self.build()
        return SeedDoc(index="registry-refs", id=doc["_id"], source=doc["_source"])


def build_bundle(lidvid: str, collection_lid: str, title: str = "Test Bundle") -> Dict:
    return (
        ProductDocumentBuilder(lidvid=lidvid, product_class="Product_Bundle")
        .with_collection_lid_reference(collection_lid)
        .with_title(title)
        .build()
    )


def build_collection(
    lidvid: str, title: str = "Test Collection", ancestry_version: Optional[int] = None
) -> Dict:
    builder = ProductDocumentBuilder(
        lidvid=lidvid, product_class="Product_Collection"
    ).with_title(title)
    if ancestry_version is not None:
        builder = builder.with_ancestry_version(ancestry_version)
    return builder.build()


def build_product(
    lidvid: str,
    product_class: str = "Product_Observational",
    title: str = "Test Product",
) -> Dict:
    return (
        ProductDocumentBuilder(lidvid=lidvid, product_class=product_class)
        .with_title(title)
        .build()
    )
