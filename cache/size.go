package cache

import "reflect"

// DeepSizeOf calculates the approximate memory size of a value in bytes, including nested structures.
//
// It recursively traverses the value's structure using reflection, accounting for the size of the type
// and its contents (e.g., strings, slices, arrays, maps, structs, and pointers). Cyclic references are
// handled to prevent infinite recursion. For pointers, maps, and other reference types, it includes the
// size of the referenced data only once. Basic types (e.g., int, bool) return their type size as defined
// by reflect.Type.Size().
//
// Parameters:
//   - v: The value to calculate the memory size for, which can be of any type.
//
// Returns:
//   - The approximate size of the value in bytes as a uintptr.
func DeepSizeOf(v any) uintptr {
	if v == nil {
		return 0
	}
	val := reflect.ValueOf(v)
	typ := val.Type()
	return deepSizeOfValue(val, typ, make(map[uintptr]bool))
}

// deepSizeOfValue recursively calculates the memory size of a reflect.Value and its nested contents.
//
// This internal function handles the recursive traversal of values, accounting for different kinds
// (e.g., pointers, slices, maps, structs) and tracking visited pointers to avoid infinite recursion
// due to cyclic references. It calculates sizes based on the type's base size (via reflect.Type.Size())
// and the sizes of nested elements, such as string contents, slice elements, or struct fields.
//
// Parameters:
//   - val: The reflect.Value to calculate the size for.
//   - typ: The reflect.Type of the value.
//   - visited: A map tracking pointers to prevent revisiting cyclic references.
//
// Returns:
//   - The approximate size of the value in bytes as a uintptr.
func deepSizeOfValue(val reflect.Value, typ reflect.Type, visited map[uintptr]bool) uintptr {
	if !val.IsValid() {
		return 0
	}

	kind := val.Kind()
	if kind == reflect.Ptr || kind == reflect.Map || kind == reflect.Chan || kind == reflect.Func {
		if val.IsNil() {
			return 0
		}

		ptr := val.Pointer()
		if visited[ptr] {
			return 0
		}
		visited[ptr] = true

		if kind == reflect.Ptr {
			return typ.Size() + deepSizeOfValue(val.Elem(), val.Elem().Type(), visited)
		}
	}

	switch kind {
	case reflect.String:
		str := val.String()
		return typ.Size() + uintptr(len(str))

	case reflect.Slice:
		if val.IsNil() {
			return typ.Size()
		}
		sliceSize := typ.Size()
		length := val.Len()
		elemType := typ.Elem()
		for i := range length {
			sliceSize += deepSizeOfValue(val.Index(i), elemType, visited)
		}
		return sliceSize

	case reflect.Array:
		arraySize := uintptr(0)
		length := val.Len()
		elemType := typ.Elem()
		for i := range length {
			arraySize += deepSizeOfValue(val.Index(i), elemType, visited)
		}
		return arraySize

	case reflect.Map:
		if val.IsNil() {
			return typ.Size()
		}

		mapSize := typ.Size()
		keyType := typ.Key()
		elemType := typ.Elem()

		iter := val.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			mapSize += deepSizeOfValue(k, keyType, visited)
			mapSize += deepSizeOfValue(v, elemType, visited)
		}

		return mapSize

	case reflect.Struct:
		structSize := uintptr(0)
		numFields := val.NumField()
		for i := range numFields {
			field := val.Field(i)
			fieldType := typ.Field(i).Type
			fieldSize := deepSizeOfValue(field, fieldType, visited)
			structSize += fieldSize
		}

		return structSize

	case reflect.Interface:
		if val.IsNil() {
			return typ.Size()
		}
		return typ.Size() + deepSizeOfValue(val.Elem(), val.Elem().Type(), visited)
	default:
		return typ.Size()
	}
}
