from ai.chronon.repo.entity_register import Entity, EntityRegister

listing = Entity(
    name="listing",
    description="A listing is a product that is for sale on the platform.",
    default=["listing_id", "id_listing", "listing"],
)

merchant = Entity(
    name="merchant",
    description="A merchant is a seller on the platform.",
    default=["merchant_id", "id_merchant", "merchant"],
)

user = Entity(
    name="customer",
    description="A user is a customer on the platform.",
)

entity_register = EntityRegister()
entity_register.register_entity(listing)
entity_register.register_entity(merchant)
entity_register.register_entity(user)