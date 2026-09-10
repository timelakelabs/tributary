<!--
The sections below are the ones the umbrella CLAUDE.md asks for. Delete the
ones that genuinely do not apply; a heading with nothing under it is worse
than no heading.

Compat is the one that is CHECKED. If this touches a file in
.github/persisted-formats.txt (the checkpoint, the spool), CI reads the line
below and cross-checks it against the diff.
-->

Compat: none

<!--
  none      nothing persisted changed shape.
  additive  an OLDER agent still reads new data CORRECTLY.

            Careful. A field an older reader silently drops is additive
            only if dropping it is HARMLESS. If it carries an instruction —
            a position to skip to, a file to forget, a segment to retire —
            then an older reader ignoring it does the wrong thing quietly,
            and that is `breaking` however optional the field looks. Three
            releases of TimeLakeDB manifest fields were waved through as
            additive on exactly that reasoning (timelakedb#160). The
            checkpoint here is built the same way, field by field.

  breaking  it does not. Bump the format version in the same change, and add:

              Downgrade: <how an operator gets back, or that they cannot>

            For a checkpoint, "delete <stream>.checkpoint and the agent
            re-reads from the start of each file, so lines are duplicated,
            not lost" is a complete answer. For the spool, say what happens
            to segments the old agent cannot read.
-->

## What changed, and why

## The risky part

<!-- You know which hunk you would want a second pair of eyes on. Naming it
     is not weakness; making a reviewer hunt for it is how it gets
     rubber-stamped. -->

## Verified

<!-- The actual output. Test counts, the drill that went red then green.
     "I tested it" is not evidence and should not read as if it were. -->

## Blast radius

<!-- What else touches this, and what breaks if it is wrong. If the answer
     is genuinely nothing, say how you know. -->

## What I left out, and why

## What I am not sure about
