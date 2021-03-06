package mongodbstorage

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var indexPrefix = "mitum_"

var manifestIndexModels = []mongo.IndexModel{
	{
		Keys: bson.D{bson.E{Key: "height", Value: 1}},
		Options: options.Index().
			SetName("mitum_manifest_height").
			SetUnique(true),
	},
}

var operationIndexModels = []mongo.IndexModel{
	{
		Keys: bson.D{bson.E{Key: "fact_hash_string", Value: 1}},
		Options: options.Index().
			SetName("mitum_operation_fact_hash").
			SetUnique(true),
	},
	{
		Keys: bson.D{bson.E{Key: "height", Value: 1}},
		Options: options.Index().
			SetName("mitum_operation_height"),
	},
}

var stateIndexModels = []mongo.IndexModel{
	{
		Keys: bson.D{bson.E{Key: "key", Value: 1}, bson.E{Key: "height", Value: 1}},
		Options: options.Index().
			SetName("mitum_state_key_and_height").
			SetUnique(true),
	},
	{
		Keys: bson.D{bson.E{Key: "height", Value: 1}},
		Options: options.Index().
			SetName("mitum_state_height"),
	},
}

var proposalIndexModels = []mongo.IndexModel{
	{
		Keys: bson.D{bson.E{Key: "hash_string", Value: 1}},
		Options: options.Index().
			SetName("mitum_proposal_hash").
			SetUnique(true),
	},
	{
		Keys: bson.D{bson.E{Key: "height", Value: 1}, bson.E{Key: "round", Value: 1}, bson.E{Key: "proposer", Value: 1}},
		Options: options.Index().
			SetName("mitum_proposal_height_round_proposer"),
	},
}

var sealIndexModels = []mongo.IndexModel{
	{
		Keys: bson.D{bson.E{Key: "hash_string", Value: 1}},
		Options: options.Index().
			SetName("mitum_seal_hash").
			SetUnique(true),
	},
	{
		Keys: bson.D{bson.E{Key: "inserted_at", Value: -1}},
		Options: options.Index().
			SetName("mitum_seal_inserted_at"),
	},
}

var defaultIndexes = map[string] /* collection */ []mongo.IndexModel{
	ColNameManifest:  manifestIndexModels,
	ColNameOperation: operationIndexModels,
	ColNameProposal:  proposalIndexModels,
	ColNameSeal:      sealIndexModels,
	ColNameState:     stateIndexModels,
}
