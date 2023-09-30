package fix

import scalafix.v1._

import scala.meta._

class PekkoScalafix extends SemanticRule("PekkoScalafix") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    //    println("Tree.syntax: " + doc.tree.syntax)
    //    println("Tree.structure: " + doc.tree.structure)
    //    println("Tree.structureLabeled: " + doc.tree.structureLabeled)
    doc.tree.collect {
      case i@Importer(ref, _) if ref.toString.startsWith("akka.stream.alpakka") =>
        Patch.replaceTree(i, i.toString()
          .replaceFirst("akka.stream.alpakka", "org.apache.pekko.stream.connectors")
          .replaceFirst("Akka", "Pekko")
        )
      case i@Importer(ref, _) if ref.toString.startsWith("akka") =>
        Patch.replaceTree(i, i.toString()
          .replaceFirst("akka", "org.apache.pekko")
          .replaceFirst("Akka", "Pekko")
        )
      case n: Type.Name if n.value.startsWith("Akka") =>
        Patch.replaceTree(n, n.toString().replaceFirst("Akka", "Pekko"))
      case n: Term.Name if n.value.contains("Akka") =>
        Patch.replaceTree(n, n.toString().replaceAll("Akka", "Pekko"))
    }.asPatch
  }

}
