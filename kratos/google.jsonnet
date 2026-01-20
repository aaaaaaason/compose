local claims = std.extVar('claims');
{
  identity: {
    traits: {
      email: claims.email,
      // Map other fields if your schema supports them
      first_name: claims.given_name,
      last_name: claims.family_name,
    },
  },
}
