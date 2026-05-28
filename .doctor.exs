%Doctor.Config{
  ignore_modules: [
    # Doctor's AST traversal counts the `def process_all` inside the macro
    # `quote` blocks of `WalEx.Event.Dsl` as functions of that module, but they
    # actually live on the caller module that uses the DSL. The defmacros
    # themselves are documented — there is no way to fix this within Doctor.
    WalEx.Event.Dsl
  ],
  ignore_paths: [~r"^test/"],
  min_module_doc_coverage: 40,
  min_module_spec_coverage: 0,
  min_overall_doc_coverage: 50,
  min_overall_moduledoc_coverage: 100,
  min_overall_spec_coverage: 0,
  exception_moduledoc_required: true,
  raise: false,
  reporter: Doctor.Reporters.Full,
  struct_type_spec_required: true,
  umbrella: false
}
