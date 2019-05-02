{ mkDerivation, base, containers, exceptions, stm, mtl, stdenv }:
mkDerivation {
  pname = "pure-locker";
  version = "0.7.0.0";
  src = ./.;
  libraryHaskellDepends = [ base containers exceptions stm mtl ];
  homepage = "github.com/grumply/pure-locker";
  description = "Shared resource management";
  license = stdenv.lib.licenses.bsd3;
}
