package io.github.party.errors

import io.github.party.network.Identifier

enum CamilError:
  case ExpectLocalValue(key: Identifier, peerType: String)
