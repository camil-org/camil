package io.github.party.peers

import scala.quoted.{ Expr, Quotes, Type }

object Peers:
  opaque type PeerTag[-P] = PeerReprImpl

  extension [P <: Peer](peerRepr: PeerTag[P])
    def baseTypeRepr: String = peerRepr.baseTypeRepr
    infix def <:<[R <: Peer](base: PeerTag[R]): Boolean =
      peerRepr.baseTypeRepr == base.baseTypeRepr || peerRepr.supertypes.contains(base.baseTypeRepr)

  private final case class PeerReprImpl(baseTypeRepr: String, supertypes: List[String]):
    override def toString: String = s"'$baseTypeRepr'${supertypes.mkString(" <: '", ", ", "'")}"

  inline given peer[T <: Peer]: PeerTag[T] = ${ peerReprImpl[T] }

  private def peerReprImpl[T: Type](using quotes: Quotes): Expr[PeerTag[T]] =
    import quotes.reflect.*

    def collectBasesOfType(tpe: TypeRepr): List[Symbol] =
      tpe match
        case Refinement(parent, _, _) =>
          collectBasesOfType(parent)
        case AndType(left, right) =>
          collectBasesOfType(left) ++ collectBasesOfType(right)
        case _ if tpe.typeSymbol.exists =>
          collectBasesOfSymbol(tpe.typeSymbol)
        case _ =>
          List.empty

    def collectBasesOfSymbol(symbol: Symbol): List[Symbol] =
      symbol.info match
        case TypeBounds(_, hi) =>
          symbol :: collectBasesOfType(hi)
        case _ =>
          List.empty

    val types = collectBasesOfSymbol(TypeRepr.of[T].typeSymbol).map(_.fullName)
    '{ PeerReprImpl(${ Expr(types.head) }, ${ Expr(types.tail) }) }
  end peerReprImpl

  type Peer = { type Tie }

  type TiedSingleWith[P <: Peer] = { type Tie <: Cardinality.Single[P] }
  type TiedManyWith[P <: Peer] = { type Tie <: Cardinality.Multiple[P] }
  type TiedWith[P <: Peer] = TiedSingleWith[P] | TiedManyWith[P]

  enum Cardinality[+P <: Peer]:
    case Single()
    case Multiple()
end Peers
