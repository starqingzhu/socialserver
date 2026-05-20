package balloon

import (
	"fmt"

	mongodbmodule "golib/mongodb"
	"golib/zaplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB collection 名称
const (
	CollBalloonActivity = "balloon_activity" // 活动注册表
	CollBalloonGroup    = "balloon_group"    // 分组数据
	CollBalloonMember   = "balloon_member"   // 成员→分组映射
	CollBalloonRobot    = "balloon_robot"    // 机器人状态
)

// DAO 封装 balloon 业务层的 MongoDB 持久化操作。
type DAO struct {
	dbName string
}

func NewDAO(dbName string) *DAO {
	if dbName == "" {
		return nil
	}
	return &DAO{dbName: dbName}
}

func (d *DAO) available() bool { return d != nil && d.dbName != "" }

func (d *DAO) session() *mongodbmodule.Session {
	return mongodbmodule.Main.TakeSession()
}

// --- 活动注册表 ---

// ActivityDoc 活动注册表文档。
type ActivityDoc struct {
	BizKey string `bson:"_id"`    // "balloon:{actID}:{round}"
	Config Config `bson:"config"`
}

func (d *DAO) SaveActivity(bizKey string, cfg Config) error {
	if !d.available() {
		return nil
	}
	_, err := d.session().UpsertOne(d.dbName, CollBalloonActivity,
		bson.M{"_id": bizKey},
		bson.M{"$set": bson.M{"config": cfg}},
	)
	return err
}

func (d *DAO) LoadAllActivities() ([]ActivityDoc, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, CollBalloonActivity, bson.M{})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []ActivityDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (d *DAO) DeleteActivity(bizKey string) error {
	if !d.available() {
		return nil
	}
	_, err := d.session().DeleteOne(d.dbName, CollBalloonActivity, bson.M{"_id": bizKey})
	return err
}

// --- 分组数据 ---

// GroupDoc 分组文档。
type GroupDoc struct {
	ID      string `bson:"_id"` // "{bizId}:{groupID}"
	BizId   string `bson:"bizId"`
	GroupID int32  `bson:"groupId"`
	Group   Group  `bson:"group"`
}

func groupDocID(bizId string, groupID int32) string {
	return fmt.Sprintf("%s:%d", bizId, groupID)
}

func (d *DAO) SaveGroup(bizId string, group *Group) error {
	if !d.available() {
		return nil
	}
	docID := groupDocID(bizId, group.GroupID)
	_, err := d.session().UpsertOne(d.dbName, CollBalloonGroup,
		bson.M{"_id": docID},
		bson.M{"$set": GroupDoc{
			ID:      docID,
			BizId:   bizId,
			GroupID: group.GroupID,
			Group:   *group,
		}},
	)
	return err
}

func (d *DAO) LoadGroups(bizId string) ([]*Group, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, CollBalloonGroup, bson.M{"bizId": bizId})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []GroupDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	groups := make([]*Group, 0, len(docs))
	for _, doc := range docs {
		g := doc.Group
		groups = append(groups, &g)
	}
	return groups, nil
}

// --- 成员映射 ---

// MemberDoc 成员→分组映射文档。
type MemberDoc struct {
	ID      string `bson:"_id"` // "{bizId}:{userID}"
	BizId   string `bson:"bizId"`
	UserID  int64  `bson:"userId"`
	GroupID int32  `bson:"groupId"`
}

func memberDocID(bizId string, userID int64) string {
	return fmt.Sprintf("%s:%d", bizId, userID)
}

func (d *DAO) SaveMember(bizId string, userID int64, groupID int32) error {
	if !d.available() {
		return nil
	}
	docID := memberDocID(bizId, userID)
	_, err := d.session().UpsertOne(d.dbName, CollBalloonMember,
		bson.M{"_id": docID},
		bson.M{"$set": MemberDoc{
			ID:      docID,
			BizId:   bizId,
			UserID:  userID,
			GroupID: groupID,
		}},
	)
	return err
}

func (d *DAO) GetMember(bizId string, userID int64) (int32, bool, error) {
	if !d.available() {
		return 0, false, nil
	}
	var doc MemberDoc
	err := d.session().FindOne(d.dbName, CollBalloonMember,
		bson.M{"_id": memberDocID(bizId, userID)}, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, false, nil
		}
		return 0, false, err
	}
	return doc.GroupID, true, nil
}

func (d *DAO) LoadAllMembers(bizId string) (map[int64]int32, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, CollBalloonMember, bson.M{"bizId": bizId})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []MemberDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	result := make(map[int64]int32, len(docs))
	for _, doc := range docs {
		result[doc.UserID] = doc.GroupID
	}
	return result, nil
}

// --- 机器人状态 ---

// RobotDoc 机器人状态文档。
type RobotDoc struct {
	ID       string     `bson:"_id"` // "{bizId}:{groupID}:{memberID}"
	BizId    string     `bson:"bizId"`
	GroupID  int32      `bson:"groupId"`
	MemberID int64      `bson:"memberId"`
	State    robotState `bson:"state"`
}

func robotDocID(bizId string, groupID int32, memberID int64) string {
	return fmt.Sprintf("%s:%d:%d", bizId, groupID, memberID)
}

func (d *DAO) SaveRobots(bizId string, groupID int32, robots []*robotState) error {
	if !d.available() || len(robots) == 0 {
		return nil
	}
	for _, r := range robots {
		docID := robotDocID(bizId, groupID, r.MemberID)
		if _, err := d.session().UpsertOne(d.dbName, CollBalloonRobot,
			bson.M{"_id": docID},
			bson.M{"$set": RobotDoc{
				ID:       docID,
				BizId:    bizId,
				GroupID:  groupID,
				MemberID: r.MemberID,
				State:    *r,
			}},
		); err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) LoadRobots(bizId string, groupID int32) ([]*robotState, error) {
	if !d.available() {
		return nil, nil
	}
	cursor, err := d.session().Find(d.dbName, CollBalloonRobot,
		bson.M{"bizId": bizId, "groupId": groupID})
	if err != nil {
		return nil, err
	}
	ctx, cancel := d.session().GetDefaultContext()
	defer cancel()
	var docs []RobotDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	robots := make([]*robotState, 0, len(docs))
	for _, doc := range docs {
		r := doc.State
		robots = append(robots, &r)
	}
	return robots, nil
}

// EnsureIndexes 创建 MongoDB 索引。
func (d *DAO) EnsureIndexes() {
	if !d.available() {
		return
	}
	session := d.session()
	ctx, cancel := session.GetDefaultContext()
	defer cancel()

	indexes := map[string][]mongo.IndexModel{
		CollBalloonGroup: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
		},
		CollBalloonMember: {
			{Keys: bson.D{{Key: "bizId", Value: 1}}, Options: options.Index().SetName("idx_bizId")},
			{Keys: bson.D{{Key: "userId", Value: 1}}, Options: options.Index().SetName("idx_userId")},
		},
		CollBalloonRobot: {
			{Keys: bson.D{{Key: "bizId", Value: 1}, {Key: "groupId", Value: 1}}, Options: options.Index().SetName("idx_bizId_groupId")},
		},
	}
	for coll, idxs := range indexes {
		if _, err := session.Database(d.dbName).Collection(coll).Indexes().CreateMany(ctx, idxs); err != nil {
			zaplog.LoggerSugar.Warnf("balloon: create indexes for %s: %v", coll, err)
		}
	}
}
