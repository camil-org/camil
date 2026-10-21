package io.github.party

object EmailSystemUtils:
  type UserId = String
  type Blob = Vector[Byte]

  trait MainServerDB:
    def getAttachments(emailId: Long): List[Attachment]
    def since(id: UserId, ts: Long): List[Email]

  trait MainDB:
    def updateAttachments(attachments: List[Attachment]): Unit
    def update(emails: List[Email]): Unit
    def extractIds(emails: List[Email]): List[Long] = emails.map(_.id)
    def lastCheckout: Long

  case class Email(id: Long, subject: String, body: String, timestamp: Long, attachments: List[Attachment])

  case class Attachment(filename: String, data: Blob)

  case class ClientConfig(userId: UserId, isOnFlatRate: Boolean)

  // Mock implementations for demonstration
  def createServerDB(): MainServerDB = new MainServerDB:
    private val emails = Map(
      "user1" -> List(
        Email(1L, "Welcome", "Welcome to our service", 1000L, List.empty),
        Email(2L, "Update", "System update available", 2000L, List.empty),
        Email(3L, "Newsletter", "Monthly newsletter", 3000L, List.empty),
      ),
    )
    private val attachments: Map[Long, List[Attachment]] = Map(
      1L -> List(Attachment("welcome.pdf", Vector[Byte](1, 2, 3))),
      2L -> List(Attachment("update.zip", Vector[Byte](4, 5, 6))),
      3L -> List(Attachment("newsletter.pdf", Vector[Byte](7, 8, 9))),
    )

    def getAttachments(emailId: Long): List[Attachment] =
      println(s"[Server] Fetching attachments for email $emailId")
      attachments.getOrElse(emailId, List.empty)

    def since(id: UserId, ts: Long): List[Email] =
      println(s"[Server] Fetching emails for user $id since timestamp $ts")
      emails.getOrElse(id, List.empty).filter(_.timestamp > ts)

  def createClientDB(): MainDB = new MainDB:
    val lastCheckout: Long = 0L

    def updateAttachments(attachments: List[Attachment]): Unit =
      println(s"[Client] Updating ${attachments.size} attachments")

    def update(emails: List[Email]): Unit =
      println(s"[Client] Updating ${emails.size} emails")
end EmailSystemUtils
