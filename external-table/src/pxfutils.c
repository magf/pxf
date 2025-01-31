#include "pxfutils.h"

#if PG_VERSION_NUM >= 90400
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#endif
#include "catalog/pg_namespace.h"
#include "utils/formatting.h"
#include "utils/syscache.h"

static const char *getenv_char(const char *name, const char *default_value);
static long getenv_long(const char *name, long default_value);

/*
 * Full name of the HEADER KEY expected by the PXF service
 * Converts input string to upper case and prepends "X-GP-OPTIONS-" string
 * This will be used for all user defined parameters to be isolate from internal parameters
 */
char *
normalize_key_name(const char *key)
{
	if (!key || strlen(key) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internal error in pxfutils.c:normalize_key_name. Parameter key is null or empty.")));
	}

	return psprintf("X-GP-OPTIONS-%s", asc_toupper(pstrdup(key), strlen(key)));
}

/*
 * TypeOidGetTypename
 * Get the name of the type, given the OID
 */
char *
TypeOidGetTypename(Oid typid)
{

	Assert(OidIsValid(typid));

	HeapTuple	typtup = SearchSysCache(TYPEOID,
							ObjectIdGetDatum(typid),
							0, 0, 0);
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", typid);

	Form_pg_type typform = (Form_pg_type) GETSTRUCT(typtup);
	char	   *typname = psprintf("%s", NameStr(typform->typname));
	ReleaseSysCache(typtup);

	return typname;
}

/* Concatenate multiple literal strings using stringinfo */
char *
concat(int num_args,...)
{
	va_list		ap;
	StringInfoData str;

	initStringInfo(&str);

	va_start(ap, num_args);

	for (int i = 0; i < num_args; i++)
	{
		appendStringInfoString(&str, va_arg(ap, char *));
	}
	va_end(ap);

	return str.data;
}

static const char*
getenv_char(const char *name, const char *default_value)
{
	const char *value = getenv(name);

	return value ? value : default_value;
}

static long 
getenv_long(const char *name, long default_value)
{
	const char *value = getenv(name);

	return value ? atol(value) : default_value;
}

/* Get authority (host:port) for the PXF server URL */
char *
get_authority(void)
{
	return psprintf("%s:%d", get_pxf_host(), get_pxf_port());
}

const char *
get_pxf_protocol(void)
{
	return getenv_char(ENV_PXF_PROTOCOL, PXF_DEFAULT_PROTOCOL);
}

/* Returns the PXF Host defined in the PXF_HOST
 * environment variable or the default when undefined
 */
const char *
get_pxf_host(void)
{
	const char *hStr = getenv(ENV_PXF_HOST);
	if (hStr)
		elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_HOST, hStr);
	else
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_HOST);
	return hStr ? hStr : PXF_DEFAULT_HOST;
}

/* Returns the PXF Port defined in the PXF_PORT
 * environment variable or the default when undefined
 */
const int
get_pxf_port(void)
{
	char *endptr = NULL;
	char *pStr = getenv(ENV_PXF_PORT);
	int port = PXF_DEFAULT_PORT;

	if (pStr) {
		port = (int) strtol(pStr, &endptr, 10);

		if (pStr == endptr)
			elog(ERROR, "unable to parse PXF port number %s=%s", ENV_PXF_PORT, pStr);
		else
			elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_PORT, pStr);
	}
	else
	{
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_PORT);
	}

	return port;
}

const char *
get_pxf_ssl_keypasswd(void)
{
	return getenv_char(ENV_PXF_SSL_KEYPASSWD, PXF_DEFAULT_SSL_KEYPASSWD);
}

const char *
get_pxf_ssl_cacert(void)
{
	return getenv_char(ENV_PXF_SSL_CACERT, PXF_DEFAULT_SSL_CACERT);
}

const char *
get_pxf_ssl_cert(void)
{
	return getenv_char(ENV_PXF_SSL_CERT, PXF_DEFAULT_SSL_CERT);
}

const char *
get_pxf_ssl_key(void)
{
	return getenv_char(ENV_PXF_SSL_KEY, PXF_DEFAULT_SSL_KEY);
}

const char *
get_pxf_ssl_certtype(void)
{
	return getenv_char(ENV_PXF_SSL_CERT_TYPE, PXF_DEFAULT_SSL_CERT_TYPE);
}

long
get_pxf_ssl_verifypeer(void)
{
	return getenv_long(ENV_PXF_SSL_VERIFY_PEER, PXF_DEFAULT_SSL_VERIFY_PEER);
}

/* Returns the namespace (schema) name for a given namespace oid */
char *
GetNamespaceName(Oid nsp_oid)
{
	HeapTuple	tuple;
	Datum		nspnameDatum;
	bool		isNull;

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("schema with OID %u does not exist", nsp_oid)));

	nspnameDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspname,
								   &isNull);

	ReleaseSysCache(tuple);

	return DatumGetCString(nspnameDatum);
}
