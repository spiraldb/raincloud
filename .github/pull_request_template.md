## Summary
<!-- What changed and why. 1–3 bullets is plenty. -->

## Test plan
<!-- See CONTRIBUTING.md for the standard pre-PR gate. -->
- [ ] `ruff check`
- [ ] `python -m scripts.pipeline.validate_manifest`
- [ ] `pytest -q`
- [ ] (pipeline change only) `python -m scripts.pipeline.build <slug>` end-to-end

## Type of change
<!-- Tick one or more. -->
- [ ] New dataset (manifest entry — see [`SKILLS.md` → Adding a new dataset](../blob/develop/SKILLS.md#adding-a-new-dataset))
- [ ] New transform handler (see [`SKILLS.md` → Adding a new transform handler](../blob/develop/SKILLS.md#adding-a-new-transform-handler))
- [ ] Bug fix
- [ ] Documentation
- [ ] Tooling / CI / repo housekeeping

## Notes for the reviewer
<!-- Anything specific to look at, or context the diff doesn't make obvious. Optional. -->
