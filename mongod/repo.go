package mongod

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// MongoRepository defines a generic interface for MongoDB repository operations.
// The type parameter T represents the document type, which must be comparable.
type MongoRepository[T any] interface {
	// Create inserts a new document into the collection.
	// Returns the inserted document's ID and any error encountered.
	Create(ctx context.Context, document T) (any, error)
	// FindOneById retrieves a single document by its ObjectID.
	// Returns the document or an error if not found or if the operation fails.
	FindOneById(ctx context.Context, id primitive.ObjectID) (*T, error)
	// FindOne retrieves a single document matching the provided filter.
	// Returns the document or an error if not found or if the operation fails.
	FindOne(ctx context.Context, filter bson.D) (*T, error)
	// FindMany retrieves multiple documents matching the provided filter.
	// Returns a slice of documents or an error if the operation fails.
	FindMany(ctx context.Context, filter bson.D) ([]T, error)
	// UpdateOneById updates a single document by its ObjectID.
	// Returns an error if the operation fails.
	UpdateOneById(ctx context.Context, id primitive.ObjectID, document T, opts ...*options.FindOneAndUpdateOptions) error
	// UpdateOneByFilter updates a single document matching the provided filter.
	// Returns the updated document or an error if the operation fails.
	UpdateOneByFilter(ctx context.Context, filter bson.M, document T, opts ...*options.FindOneAndUpdateOptions) (*T, error)
	// UpdateMany updates multiple documents matching the provided filter.
	// Returns an error if the operation fails.
	UpdateMany(ctx context.Context, filter bson.D, document T) error
	// DeleteById deletes a single document by its ObjectID.
	// Returns an error if the operation fails.
	DeleteById(ctx context.Context, id primitive.ObjectID) (int64, error)

	DeleteOneByFilter(ctx context.Context, filter bson.M) (int64, error)
	// DeleteMany deletes multiple documents matching the provided filter.
	// Returns an error if the operation fails.
	DeleteMany(ctx context.Context, filter bson.D) (int64, error)
	// Count returns the number of documents matching the provided filter.
	// Returns the count or an error if the operation fails.
	Count(ctx context.Context, filter bson.D) (int64, error)
	// CreateIndex creates an index on the collection with the specified keys and options.
	// Returns the name of the created index or an error if the operation fails.
	CreateIndex(ctx context.Context, keys bson.D, opt *options.IndexOptions) (string, error)
	// EstimatedDocumentCount returns an estimated count of documents in the collection.
	// Returns the count or an error if the operation fails.
	EstimatedDocumentCount(ctx context.Context) (int64, error)
	// Aggregate performs an aggregation operation using the provided pipeline.
	// Returns a slice of documents or an error if the operation fails.
	Aggregate(ctx context.Context, pipeline mongo.Pipeline, opts ...*options.AggregateOptions) ([]*T, error)
	// Reconcile compares a provided document with the latest version in the database based on a specified filter
	// and a map of fields to compare, updating if any field differs.
	// Returns the resulting document, a boolean indicating if an update occurred, and any error encountered.
	Reconcile(ctx context.Context, document T, filter bson.M, compareFields map[string]func(a, b any) (bool, error)) (*T, bool, error)
	// GetClient returns the MongoDB client associated with the repository.
	GetClient() *mongo.Client
}

// Repository is a generic MongoDB repository implementation for performing CRUD and other operations.
// The type parameter T represents the document type.
type Repository[T any] struct {
	client     *mongo.Client     // MongoDB client for database operations
	collection *mongo.Collection // MongoDB collection to operate on
}

// RepoOption defines a function type for configuring a Repository instance.
type RepoOption[T any] func(*Repository[T])

// WithClient returns a RepoOption that sets the MongoDB client for the repository.
func WithClient[T any](client *mongo.Client) RepoOption[T] {
	return func(r *Repository[T]) { r.client = client }
}

// WithCollection returns a RepoOption that sets the MongoDB collection for the repository.
func WithCollection[T any](collection *mongo.Collection) RepoOption[T] {
	return func(r *Repository[T]) { r.collection = collection }
}

// NewRepository creates a new Repository instance for the specified MongoDB collection.
// Parameters:
//   - collection: The MongoDB collection to operate on.
//
// Returns:
//   - A Repository instance configured with the provided collection.
func NewRepository[T any](collection *mongo.Collection) Repository[T] {
	return Repository[T]{
		collection: collection,
	}
}

// NewRepositoryWithOptions creates a new Repository instance with custom configuration options.
// Parameters:
//   - opts: Variadic RepoOption functions to configure the repository.
//
// Returns:
//   - A pointer to the configured Repository instance.
func NewRepositoryWithOptions[T any](opts ...RepoOption[T]) *Repository[T] {
	repo := &Repository[T]{}
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

// Create inserts a document into the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - document: The document to insert, of type T.
//
// Returns:
//   - The inserted document's ID (typically an ObjectID).
//   - An error if the insertion fails.
func (r *Repository[T]) Create(ctx context.Context, document T) (any, error) {
	result, err := r.collection.InsertOne(ctx, document)
	if err != nil {
		return nil, err
	}
	return result.InsertedID, nil
}

// FindOneById finds a single document by its ObjectID in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - id: The ObjectID of the document to retrieve.
//
// Returns:
//   - A pointer to the found document of type T, or nil if not found.
//   - An error if the operation fails (e.g., document not found).
func (r *Repository[T]) FindOneById(ctx context.Context, id primitive.ObjectID) (*T, error) {
	filter := bson.D{{Key: "_id", Value: id}}
	var result T
	err := r.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// FindOne finds a single document matching the provided filter in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//
// Returns:
//   - A pointer to the found document of type T, or nil if not found.
//   - An error if the operation fails.
func (r *Repository[T]) FindOne(ctx context.Context, filter bson.D) (*T, error) {
	var result T
	err := r.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// FindMany finds multiple documents matching the provided filter in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//
// Returns:
//   - A slice of documents of type T.
//   - An error if the operation fails.
func (r *Repository[T]) FindMany(ctx context.Context, filter bson.D, opts ...*options.FindOptions) ([]T, error) {
	cursor, err := r.collection.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []T
	for cursor.Next(ctx) {
		var result T
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// UpdateOneById updates a single document by its ObjectID in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - id: The ObjectID of the document to update.
//   - document: The updated document data of type T.
//
// Returns:
//   - An error if the operation fails.
func (r *Repository[T]) UpdateOneById(ctx context.Context, id primitive.ObjectID, document T, opts ...*options.FindOneAndUpdateOptions) error {
	filter := bson.D{{Key: "_id", Value: id}}
	updateDoc, err := excludeIdField(document)
	if err != nil {
		return fmt.Errorf("failed to prepare update document: %w", err)
	}
	update := bson.D{{Key: "$set", Value: updateDoc}}
	result := r.collection.FindOneAndUpdate(ctx, filter, update, opts...)
	return result.Err()
}

// UpdateOneByFilter updates a single document matching the provided filter in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//   - document: The updated document data of type T.
//   - opts: Optional update options.
//
// Returns:
//   - A pointer to the updated document of type T.
//   - An error if the operation fails.
func (r *Repository[T]) UpdateOneByFilter(
	ctx context.Context, filter bson.M, document T,
	opts ...*options.FindOneAndUpdateOptions) (*T, error) {
	updateDoc, err := excludeIdField(document)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare update document: %w", err)
	}
	update := bson.D{{Key: "$set", Value: updateDoc}}
	result := r.collection.FindOneAndUpdate(ctx, filter, update, opts...)
	var val T
	err = result.Decode(&val)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// UpdateMany updates multiple documents matching the provided filter in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//   - document: The updated document data of type T.
//
// Returns:
//   - An error if the operation fails.
func (r *Repository[T]) UpdateMany(ctx context.Context, filter bson.D, document T) error {
	update := bson.D{{Key: "$set", Value: document}}
	_, err := r.collection.UpdateMany(ctx, filter, update)
	return err
}

// DeleteById deletes a single document by its ObjectID from the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - id: The ObjectID of the document to delete.
//
// Returns:
//   - An error if the operation fails.
func (r *Repository[T]) DeleteById(ctx context.Context, id primitive.ObjectID) (int64, error) {
	filter := bson.D{{Key: "_id", Value: id}}
	result, err := r.collection.DeleteOne(ctx, filter)
	return result.DeletedCount, err
}

func (r *Repository[T]) DeleteOneByFilter(ctx context.Context, filter bson.M) (int64, error) {
	result, err := r.collection.DeleteOne(ctx, filter)
	return result.DeletedCount, err
}

// DeleteMany deletes multiple documents matching the provided filter from the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//
// Returns:
//   - An error if the operation fails.
func (r *Repository[T]) DeleteMany(ctx context.Context, filter bson.D) (int64, error) {
	result, err := r.collection.DeleteMany(ctx, filter)
	return result.DeletedCount, err
}

// Count returns the number of documents matching the provided filter in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - filter: A BSON filter to match documents.
//
// Returns:
//   - The count of matching documents.
//   - An error if the operation fails.
func (r *Repository[T]) Count(ctx context.Context, filter bson.D) (int64, error) {
	count, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CreateIndex creates an index in the MongoDB collection with the specified keys and options.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - keys: A BSON document specifying the fields to index.
//   - opt: Optional index options (e.g., unique, sparse).
//
// Returns:
//   - The name of the created index.
//   - An error if the operation fails.
func (r *Repository[T]) CreateIndex(ctx context.Context, keys bson.D, opt *options.IndexOptions) (string, error) {
	index := mongo.IndexModel{
		Keys:    keys,
		Options: opt,
	}
	return r.collection.Indexes().CreateOne(ctx, index)
}

// EstimatedDocumentCount returns an estimated count of all documents in the MongoDB collection.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//
// Returns:
//   - The estimated document count.
//   - An error if the operation fails.
func (r *Repository[T]) EstimatedDocumentCount(ctx context.Context) (int64, error) {
	count, err := r.collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Aggregate performs an aggregation operation on the MongoDB collection using the provided pipeline.
// Parameters:
//   - ctx: The context for controlling cancellation and deadlines.
//   - pipeline: The MongoDB aggregation pipeline.
//   - opts: Optional aggregation options.
//
// Returns:
//   - A slice of pointers to documents of type T resulting from the aggregation.
//   - An error if the operation fails.
func (r *Repository[T]) Aggregate(ctx context.Context, pipeline mongo.Pipeline, opts ...*options.AggregateOptions) ([]*T, error) {
	cursor, err := r.collection.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []*T
	for cursor.Next(ctx) {
		var result T
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		results = append(results, &result)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type ReconcilationSpec map[string]func(a any, b any) (bool, error)

// Reconcile compares a provided document with the latest version in the database based on a specified filter
// and a map of fields to compare, updating if any field differs.
// Parameters:
//   - ctx: Context for controlling cancellation and deadlines.
//   - document: The document to compare and potentially update, of type T.
//   - filter: A BSON filter to locate the document in the database.
//   - compareFields: A map of field names to comparison functions that determine if an update is needed.
//     Each function takes two values (new and old) and returns a boolean indicating if they differ and an error if the comparison fails.
//
// Returns:
//   - A pointer to the resulting document (either the original or updated).
//   - A boolean indicating if an update occurred.
//   - An error if the operation fails.
func (r *Repository[T]) Reconcile(ctx context.Context, document T, filter bson.M, compareFields ReconcilationSpec) (*T, bool, error) {
	result := r.collection.FindOne(ctx, filter, options.FindOne().SetAllowPartialResults(false))
	var oldDocument T
	if err := result.Decode(&oldDocument); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("could not decode retrieved document for comparison: %w", err)
	}

	newVal := reflect.ValueOf(document)
	oldVal := reflect.ValueOf(oldDocument)

	if newVal.Type() != oldVal.Type() {
		return nil, false, fmt.Errorf("document types do not match: new=%T, old=%T", document, oldDocument)
	}

	if newVal.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("document must be a struct, got %v", newVal.Kind())
	}

	needUpdate := true

	for key, cmpFunc := range compareFields {
		// Get field by name
		newField := newVal.FieldByName(key)
		oldField := oldVal.FieldByName(key)

		// Check if field exists
		if !newField.IsValid() || !oldField.IsValid() {
			return nil, false, fmt.Errorf("field %s not found in document type %T", key, document)
		}

		// Get field values as interface{}
		newFieldVal := newField.Interface()
		oldFieldVal := oldField.Interface()

		// Apply comparison function
		ret, err := cmpFunc(oldFieldVal, newFieldVal)
		if err != nil {
			return nil, false, fmt.Errorf("comparison failed for field %s: %w", key, err)
		}
		// Update is needed if any comparison returns true
		needUpdate = needUpdate && ret
	}

	if needUpdate {
		returnDocument, err := r.UpdateOneByFilter(
			ctx, filter, document, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After))
		if err != nil {
			return nil, true, fmt.Errorf("failed to decode updated document: %w", err)
		}
		return returnDocument, true, nil
	}

	return &oldDocument, needUpdate, nil
}

// GetClient returns the MongoDB client associated with the repository.
// Returns:
//   - The MongoDB client.
func (r *Repository[T]) GetClient() *mongo.Client { return r.client }

func (r *Repository[T]) WrapWithTransaction(
	ctx context.Context,
	data T, callback func(ctx mongo.SessionContext, repo *Repository[T], data T) error,
	sessionOpts ...*options.SessionOptions) error {
	session, err := r.GetClient().StartSession(sessionOpts...)
	if err != nil {
		return fmt.Errorf("failed to start session : %w", err)
	}
	defer session.EndSession(ctx)
	err = session.StartTransaction(options.Transaction().SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		return fmt.Errorf("failed to start transaction : %w", err)
	}
	err = mongo.WithSession(ctx, session, func(ctx mongo.SessionContext) error {
		err := callback(ctx, r, data)
		if err != nil {
			return fmt.Errorf("error from callback : %w", err)
		}
		return session.CommitTransaction(ctx)
	})

	if err != nil {
		if abortErr := session.AbortTransaction(ctx); abortErr != nil {
			return abortErr
		}
		return err
	}
	return nil
}

func excludeIdField[T any](document T) (bson.M, error) {
	data, err := bson.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	var bsonMap bson.M
	if err := bson.Unmarshal(data, &bsonMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	delete(bsonMap, "_id")
	val := reflect.ValueOf(document)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return bsonMap, nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		bsonTag := field.Tag.Get("bson")
		if bsonTag == "" {
			continue
		}

		var bsonFieldName string
		hasOmitZero := false
		for tagPart := range strings.SplitSeq(bsonTag, ",") {
			if tagPart == "omitzero" {
				hasOmitZero = true
			} else if tagPart != "omitempty" && tagPart != "" {
				bsonFieldName = tagPart
			}
		}

		if hasOmitZero && bsonFieldName != "" {
			fieldValue := val.Field(i)
			if fieldValue.IsZero() {
				delete(bsonMap, bsonFieldName)
			}
		}
	}

	if len(bsonMap) == 0 {
		return nil, fmt.Errorf("no fields to update after excluding _id and zero-valued fields")
	}
	return bsonMap, nil
}
