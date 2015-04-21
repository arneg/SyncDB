inherit .Binary;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Image();
}
#endif

string type_name() {
    return "image";
}
