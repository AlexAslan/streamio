package options

import (
	"errors"
	"fmt"
	"strings"

	"github.com/AlexAslan/streamio/internal/record"
)

// PathTransformRule is one declarative path-level transform. Use RenamePath and DropPath to
// build rules so invalid partial rules cannot be constructed accidentally.
type PathTransformRule struct {
	from string
	to   string
	drop bool
}

// RenamePath returns a transform rule that changes one field path. Dotted paths address nested
// map entries, for example "attributes.service".
func RenamePath(from, to string) PathTransformRule {
	return PathTransformRule{from: from, to: to}
}

// DropPath returns a transform rule that removes one field path. Dotted paths address nested map
// entries, for example "attributes.debug".
func DropPath(name string) PathTransformRule {
	return PathTransformRule{from: name, drop: true}
}

var (
	errEmptyPath              = errors.New("empty path")
	errEmptyPathSegment       = errors.New("empty path segment")
	errRenameChangesPathDepth = errors.New("rename changes path depth")
	errRenameChangesParent    = errors.New("rename changes path parent")
	errDuplicateTransformRule = errors.New("duplicate transform rule")
	errDropHasDescendantRule  = errors.New("drop path conflicts with a rule under it")
)

type fieldAction struct {
	children map[string]*fieldAction
	rename   string
	drop     bool
	terminal bool
}

// PathTransformer applies compiled rename/drop rules in one pass over a record. Rules are stored
// as a path trie so per-record work checks only the fields that can match at each level.
type PathTransformer struct {
	byHead map[string]*fieldAction
}

// NewPathTransformer compiles field rules once, before any records are processed.
func NewPathTransformer(rules ...PathTransformRule) (*PathTransformer, error) {
	root := make(map[string]*fieldAction, len(rules))
	for _, rule := range rules {
		from, err := parseFieldPath(rule.from)
		if err != nil {
			return nil, fmt.Errorf("streamio: invalid transform source %q: %w", rule.from, err)
		}

		var rename string
		if !rule.drop {
			to, parseErr := parseFieldPath(rule.to)
			if parseErr != nil {
				return nil, fmt.Errorf("streamio: invalid rename target %q: %w", rule.to, parseErr)
			}
			if len(to) != len(from) {
				return nil, fmt.Errorf("%w: from %q to %q", errRenameChangesPathDepth, rule.from, rule.to)
			}
			if len(to) > 1 && strings.Join(to[:len(to)-1], ".") != strings.Join(from[:len(from)-1], ".") {
				return nil, fmt.Errorf("%w: from %q to %q", errRenameChangesParent, rule.from, rule.to)
			}
			rename = to[len(to)-1]
		}

		insertErr := insertFieldAction(root, from, rename, rule.drop)
		if insertErr != nil {
			return nil, fmt.Errorf("streamio: transform for field path %q: %w", rule.from, insertErr)
		}
	}

	// transformRecord short-circuits on action.drop before ever descending into action.children,
	// so a rule configured under a dropped path would be compiled but never applied. Reject that
	// combination here instead of silently ignoring it, regardless of which rule was added first.
	if err := checkNoDropHasDescendants(root); err != nil {
		return nil, err
	}

	return &PathTransformer{byHead: root}, nil
}

// checkNoDropHasDescendants reports an error if any drop action in the trie has children, which
// only happens when another rule's path passes through the dropped field.
func checkNoDropHasDescendants(actions map[string]*fieldAction) error {
	for name, action := range actions {
		if action.drop && len(action.children) > 0 {
			return fmt.Errorf("streamio: %w: %q", errDropHasDescendantRule, name)
		}
		if err := checkNoDropHasDescendants(action.children); err != nil {
			return err
		}
	}
	return nil
}

func parseFieldPath(path string) ([]string, error) {
	if path == "" {
		return nil, errEmptyPath
	}
	parts := strings.Split(path, ".")
	for _, part := range parts {
		if part == "" {
			return nil, errEmptyPathSegment
		}
	}
	return parts, nil
}

func insertFieldAction(root map[string]*fieldAction, path []string, rename string, drop bool) error {
	actions := root
	var action *fieldAction
	for _, segment := range path {
		action = actions[segment]
		if action == nil {
			action = &fieldAction{}
			actions[segment] = action
		}
		if action.children == nil {
			action.children = make(map[string]*fieldAction)
		}
		actions = action.children
	}
	if action == nil {
		return errEmptyPath
	}
	if action.terminal {
		return errDuplicateTransformRule
	}
	action.terminal = true
	action.drop = drop
	action.rename = rename
	return nil
}

// Transform applies configured rename/drop actions to rec.
func (t *PathTransformer) Transform(rec record.Record) (record.Record, error) {
	if t == nil || len(t.byHead) == 0 {
		return rec, nil
	}
	return transformRecord(rec, t.byHead), nil
}

func transformRecord(rec record.Record, actions map[string]*fieldAction) record.Record {
	// Safe in-place filter: out shares rec's backing array, but the write index (len(out)) never
	// outpaces the read index (range's position in rec) since every iteration appends at most the
	// one field it just read, so out never overwrites a field before rec's range loop reads it.
	out := rec[:0]
	for _, field := range rec {
		action := actions[field.Name]
		if action == nil {
			out = append(out, field)
			continue
		}

		if action.drop {
			continue
		}
		if field.Value.Kind == record.KindMap && len(action.children) > 0 {
			field.Value.Map = transformRecord(field.Value.Map, action.children)
		}
		if action.terminal && action.rename != "" {
			field.Name = action.rename
		}
		out = append(out, field)
	}
	return out
}
