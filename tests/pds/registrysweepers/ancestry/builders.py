"""Test data builders for ancestry module tests.

These builders provide a clean, maintainable way to construct test data
for OpenSearch documents without relying on JSON files.
"""
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import List
from typing import Optional


@dataclass
class ProductDocumentBuilder:
    """Builder for OpenSearch product documents.

    Constructs documents that match the structure returned by OpenSearch
    queries in the ancestry module.

    Example:
        collection = ProductDocumentBuilder(
            lidvid="urn:nasa:pds:bundle:collection::1.0",
            product_class="Product_Collection"
        ).with_ancestry_version(6).build()
    """
    lidvid: str
    product_class: str
    title: str = "Test Product"
    ops_ancestry_version: Optional[int] = None
    alternate_ids: List[str] = field(default_factory=list)
    ref_lid_collection: List[str] = field(default_factory=list)
    ref_lidvid_collection: List[str] = field(default_factory=list)

    def with_ancestry_version(self, version: int) -> "ProductDocumentBuilder":
        """Set the ancestry version for this product.

        Args:
            version: The ancestry version number

        Returns:
            Self for method chaining
        """
        self.ops_ancestry_version = version
        return self

    def with_alternate_id(self, alt_id: str) -> "ProductDocumentBuilder":
        """Add an alternate ID to this product.

        Alternate IDs are used to link bundles to collections via LID references.

        Args:
            alt_id: The alternate identifier (typically a LID)

        Returns:
            Self for method chaining
        """
        self.alternate_ids.append(alt_id)
        return self

    def with_title(self, title: str) -> "ProductDocumentBuilder":
        """Set the title for this product.

        Args:
            title: The product title

        Returns:
            Self for method chaining
        """
        self.title = title
        return self

    def with_collection_lid_reference(self, collection_lid: str) -> "ProductDocumentBuilder":
        """Add a collection LID reference (for bundles referencing collections by LID).

        Args:
            collection_lid: The LID of the referenced collection

        Returns:
            Self for method chaining
        """
        self.ref_lid_collection.append(collection_lid)
        return self

    def with_collection_lidvid_reference(self, collection_lidvid: str) -> "ProductDocumentBuilder":
        """Add a collection LIDVID reference (for bundles referencing collections by LIDVID).

        Args:
            collection_lidvid: The LIDVID of the referenced collection

        Returns:
            Self for method chaining
        """
        self.ref_lidvid_collection.append(collection_lidvid)
        return self

    def build(self) -> Dict:
        """Build the OpenSearch hit document.

        Returns:
            Dict with _source and _id fields matching OpenSearch response format
        """
        source = {
            "lidvid": self.lidvid,
            "product_class": self.product_class,
            "title": self.title,
            "alternate_ids": self.alternate_ids,
        }

        if self.ops_ancestry_version is not None:
            source["ops:Sweepers/ops:ancestry_version"] = self.ops_ancestry_version

        if self.ref_lid_collection:
            source["ref_lid_collection"] = self.ref_lid_collection

        if self.ref_lidvid_collection:
            source["ref_lidvid_collection"] = self.ref_lidvid_collection

        return {"_source": source, "_id": self.lidvid}


@dataclass
class CollectionRefsBuilder:
    """Builder for registry-refs documents.

    Constructs documents from the registry-refs index that link collections
    to their member products.

    Example:
        refs = CollectionRefsBuilder("urn:nasa:pds:collection::1.0")
            .with_product("urn:nasa:pds:collection:product1::1.0")
            .with_product("urn:nasa:pds:collection:product2::1.0")
            .build()
    """
    collection_lidvid: str
    product_lidvids: List[str] = field(default_factory=list)
    batch_id: int = 1

    def with_product(self, lidvid: str) -> "CollectionRefsBuilder":
        """Add a product member to this collection.

        Args:
            lidvid: The LIDVID of the member product

        Returns:
            Self for method chaining
        """
        self.product_lidvids.append(lidvid)
        return self

    def with_products(self, lidvids: List[str]) -> "CollectionRefsBuilder":
        """Add multiple product members to this collection.

        Args:
            lidvids: List of LIDVIDs of member products

        Returns:
            Self for method chaining
        """
        self.product_lidvids.extend(lidvids)
        return self

    def with_batch_id(self, batch_id: int) -> "CollectionRefsBuilder":
        """Set the batch ID for this refs document.

        Args:
            batch_id: The batch identifier

        Returns:
            Self for method chaining
        """
        self.batch_id = batch_id
        return self

    def build(self) -> Dict:
        """Build the registry-refs document.

        Returns:
            Dict with _source and _id fields matching OpenSearch response format
        """
        return {
            "_id": f"{self.collection_lidvid}::batch_{self.batch_id}",
            "_source": {
                "collection_lidvid": self.collection_lidvid,
                "batch_id": self.batch_id,
                "product_lidvid": self.product_lidvids
            }
        }


def build_bundle(lidvid: str, collection_lid: str, title: str = "Test Bundle") -> Dict:
    """Convenience function to build a bundle with a collection reference.

    Args:
        lidvid: The bundle LIDVID
        collection_lid: The LID of the collection this bundle references
        title: Optional title for the bundle

    Returns:
        OpenSearch document dict
    """
    return ProductDocumentBuilder(
        lidvid=lidvid,
        product_class="Product_Bundle"
    ).with_collection_lid_reference(collection_lid).with_title(title).build()


def build_collection(lidvid: str, title: str = "Test Collection", ancestry_version: Optional[int] = None) -> Dict:
    """Convenience function to build a collection.

    Args:
        lidvid: The collection LIDVID
        title: Optional title for the collection
        ancestry_version: Optional existing ancestry version

    Returns:
        OpenSearch document dict
    """
    builder = ProductDocumentBuilder(
        lidvid=lidvid,
        product_class="Product_Collection"
    ).with_title(title)

    if ancestry_version is not None:
        builder = builder.with_ancestry_version(ancestry_version)

    return builder.build()


def build_product(lidvid: str, product_class: str = "Product_Observational", title: str = "Test Product") -> Dict:
    """Convenience function to build a non-aggregate product.

    Args:
        lidvid: The product LIDVID
        product_class: The product class (default: Product_Observational)
        title: Optional title for the product

    Returns:
        OpenSearch document dict
    """
    return ProductDocumentBuilder(
        lidvid=lidvid,
        product_class=product_class
    ).with_title(title).build()
