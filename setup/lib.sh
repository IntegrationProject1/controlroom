#!/usr/bin/env bash

# Simpele logging-functies
function log {
	echo "[+] $1"
}

function sublog {
	echo "   ⠿ $1"
}

function err {
	echo "[x] $1" >&2
}

function suberr {
	echo "   ⠍ $1" >&2
}

# Wacht tot Elasticsearch beschikbaar is (HTTP 200)
function wait_for_elasticsearch {
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=( '-s' '-D-' '-m15' '-w' '%{http_code}' "http://${elasticsearch_host}:9200/" )

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local output

	for _ in $(seq 1 60); do
		local -i exit_code=0
		output="$(curl "${args[@]}")" || exit_code=$?

		((exit_code)) && result=$exit_code

		if [[ "${output: -3}" -eq 200 ]]; then
			result=0
			break
		fi

		sleep 5
	done

	if ((result)) && [[ "${output: -3}" -ne 000 ]]; then
		echo -e "\n${output::-3}"
	fi

	return $result
}

# Wacht tot de ingebouwde gebruikers zijn geïnitialiseerd
function wait_for_builtin_users {
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=( '-s' '-D-' '-m15' "http://${elasticsearch_host}:9200/_security/user?pretty" )

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local line
	local -i exit_code
	local -i num_users

	for _ in $(seq 1 30); do
		num_users=0

		while IFS= read -r line || ! exit_code="$line"; do
			if [[ "$line" =~ _reserved.+true ]]; then
				(( num_users++ ))
			fi
		done < <(curl "${args[@]}"; printf '%s' "$?")

		((exit_code)) && result=$exit_code
		if (( num_users > 1 )); then
			result=0
			break
		fi

		sleep 1
	done

	return $result
}

# Check of gebruiker al bestaat
function check_user_exists {
	local username=$1
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=( '-s' '-D-' '-m15' '-w' '%{http_code}' "http://${elasticsearch_host}:9200/_security/user/${username}" )

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local -i exists=0
	local output

	output="$(curl "${args[@]}")"
	[[ "${output: -3}" -eq 200 || "${output: -3}" -eq 404 ]] && result=0
	[[ "${output: -3}" -eq 200 ]] && exists=1

	((result)) && echo -e "\n${output::-3}" || echo "$exists"

	return $result
}

# Wachtwoord instellen voor bestaande gebruiker
function set_user_password {
	local username=$1
	local password=$2
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=(
		'-s' '-D-' '-m15' '-w' '%{http_code}'
		"http://${elasticsearch_host}:9200/_security/user/${username}/_password"
		'-X' 'POST'
		'-H' 'Content-Type: application/json'
		'-d' "{\"password\" : \"${password}\"}"
	)

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local output

	output="$(curl "${args[@]}")"
	[[ "${output: -3}" -eq 200 ]] && result=0

	((result)) && echo -e "\n${output::-3}\n"

	return $result
}

# Maak een nieuwe gebruiker aan met rol en wachtwoord
function create_user {
	local username=$1
	local password=$2
	local role=$3
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=(
		'-s' '-D-' '-m15' '-w' '%{http_code}'
		"http://${elasticsearch_host}:9200/_security/user/${username}"
		'-X' 'POST'
		'-H' 'Content-Type: application/json'
		'-d' "{\"password\":\"${password}\",\"roles\":[\"${role}\"]}"
	)

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local output

	output="$(curl "${args[@]}")"
	[[ "${output: -3}" -eq 200 ]] && result=0

	((result)) && echo -e "\n${output::-3}\n"

	return $result
}

# Controleer of een rol aanwezig is, maak aan of update indien nodig
function ensure_role {
	local name=$1
	local body=$2
	local elasticsearch_host="${ELASTICSEARCH_HOST:-elasticsearch}"
	local -a args=(
		'-s' '-D-' '-m15' '-w' '%{http_code}'
		"http://${elasticsearch_host}:9200/_security/role/${name}"
		'-X' 'POST'
		'-H' 'Content-Type: application/json'
		'-d' "$body"
	)

	if [[ -n "${ELASTIC_PASSWORD:-}" ]]; then
		args+=( '-u' "elastic:${ELASTIC_PASSWORD}" )
	fi

	local -i result=1
	local output

	output="$(curl "${args[@]}")"
	[[ "${output: -3}" -eq 200 ]] && result=0

	((result)) && echo -e "\n${output::-3}\n"

	return $result
}
