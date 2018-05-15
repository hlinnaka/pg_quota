/*
 *
 */

#ifndef PG_QUOTA_H
#define PG_QUOTA_H

#include "storage/relfilenode.h"

/* prototypes for pg_quota.c */
extern Oid get_relfilenode_owner(RelFileNode *rnode);

/* prototypes for fs_model.c */
extern void init_fs_model(void);
extern void init_fs_model_shmem(void);
extern void refresh_fs_model(void);

extern void UpdateRelOwner(RelFileNode *rnode, Oid owner);
extern void UpdateOrphans(void);

extern bool CheckQuota(Oid owner);
extern void UpdateQuota(Oid owner, int64 newquota);

/* prototypes for enforcement.c */
extern void init_quota_enforcement(void);

#endif							/* PG_QUOTA_H */
