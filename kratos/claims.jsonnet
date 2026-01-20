local session = std.extVar('session');
{
  claims: {
    sub: session.identity.id
  }
}
