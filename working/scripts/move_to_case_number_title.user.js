// <nowiki>
(function () {
	'use strict';

	var BUTTON_TITLE = '移動到文書號標題';
	var COPY_BUTTON_TITLE = '複製到文書號標題';
	var BUTTON_ID = 'move-to-case-number-title';
	var CASE_NUMBER_ELEMENT_ID = 'prc-court-document-case-number';
	var CREATE_PAGE_SUMMARY = 'Imported from 裁判文书网 (credit: caseopen.org)';

	function normalizeTitle(title) {
		return String(title || '').trim().replace(/_/g, ' ');
	}

	function getCurrentPageTitle() {
		return normalizeTitle(mw.config.get('wgRelevantPageName') || mw.config.get('wgPageName'));
	}

	function getPositiveConfigNumber(name) {
		var value = Number(mw.config.get(name));
		return value > 0 ? value : null;
	}

	function getUrlOldId() {
		var oldId = new URLSearchParams(location.search).get('oldid');
		return oldId && /^\d+$/.test(oldId) ? Number(oldId) : null;
	}

	function getViewedRevisionId() {
		return getPositiveConfigNumber('wgRevisionId') || getUrlOldId();
	}

	function isViewingLatestRevision() {
		var viewedRevisionId = getViewedRevisionId();
		var latestRevisionId = getPositiveConfigNumber('wgCurRevisionId');
		if (latestRevisionId) {
			return !viewedRevisionId || viewedRevisionId === latestRevisionId;
		}

		return !getUrlOldId();
	}

	function getRevisionContent(revision) {
		if (revision.slots && revision.slots.main) {
			return revision.slots.main.content || revision.slots.main['*'] || '';
		}

		return revision.content || revision['*'] || '';
	}

	function showError(error) {
		var message = error && error.message ? error.message : String(error);
		if (mw.notify) {
			mw.notify(message, { type: 'error' });
			return;
		}

		alert(message);
	}

	function reloadAfterSuccess() {
		window.setTimeout(function () {
			location.reload();
		}, 500);
	}

	function goToPage(title) {
		window.setTimeout(function () {
			if (mw.util && mw.util.getUrl) {
				location.href = mw.util.getUrl(title);
				return;
			}

			location.href = mw.config.get('wgScript') + '?title=' +
				encodeURIComponent(title.replace(/ /g, '_'));
		}, 500);
	}

	function wrapHeaderTitleValue(wikitext) {
		var headerStart = wikitext.search(/\{\{\s*Header\/裁判文书(?=\s|[|\n}])/i);
		var headerEnd;
		var closeMatch;
		var headerText;
		var matched = false;
		var changed = false;
		var updatedHeader;

		if (headerStart === -1) {
			throw new Error('在页面源码中找不到 {{Header/裁判文书}}。');
		}

		closeMatch = /\n[ \t]*\}\}/.exec(wikitext.slice(headerStart));
		headerEnd = closeMatch ? headerStart + closeMatch.index : wikitext.length;
		headerText = wikitext.slice(headerStart, headerEnd);
		updatedHeader = headerText.replace(/(^[ \t]*\|\s*title\s*=\s*)([^\n]*)(?=\n|$)/m,
			function (match, prefix, value) {
				var leadingSpace;
				var trailingSpace;
				var titleValue;

				matched = true;
				leadingSpace = value.match(/^\s*/)[0];
				trailingSpace = value.match(/\s*$/)[0];
				titleValue = value.slice(leadingSpace.length, value.length - trailingSpace.length);

				if (!titleValue || (titleValue.indexOf('[[') === 0 &&
					titleValue.lastIndexOf(']]') === titleValue.length - 2)) {
					return match;
				}

				changed = true;
				return prefix + leadingSpace + '[[' + titleValue + ']]' + trailingSpace;
			});

		if (!matched) {
			throw new Error('在 {{Header/裁判文书}} 中找不到 title 参数。');
		}

		return {
			changed: changed,
			wikitext: wikitext.slice(0, headerStart) + updatedHeader + wikitext.slice(headerEnd)
		};
	}

	function buildPatchEdit(title, wikitext, baseRevisionId) {
		var wrapped = wrapHeaderTitleValue(wikitext);
		var params;

		if (!wrapped.changed) {
			return null;
		}

		params = {
			action: 'edit',
			title: title,
			text: wrapped.wikitext,
			summary: '',
			nocreate: 1,
			formatversion: 2
		};

		if (baseRevisionId) {
			params.baserevid = baseRevisionId;
		}

		return params;
	}

	function fetchRevisionWikitext(api, revisionId) {
		if (!revisionId) {
			throw new Error('无法确定当前查看的修订版本。');
		}

		return api.get({
			action: 'query',
			prop: 'revisions',
			revids: revisionId,
			rvprop: 'ids|content',
			rvslots: 'main',
			formatversion: 2
		}).then(function (data) {
			var page = data.query.pages[0];
			var revision = page.revisions && page.revisions[0];
			if (!revision) {
				throw new Error('无法读取当前修订版本源码：' + revisionId);
			}

			return {
				revid: revision.revid,
				wikitext: getRevisionContent(revision)
			};
		});
	}

	function fetchPageWikitext(api, title) {
		return api.get({
			action: 'query',
			prop: 'revisions',
			titles: title,
			rvprop: 'ids|content',
			rvslots: 'main',
			formatversion: 2
		}).then(function (data) {
			var page = data.query.pages[0];
			var revision = page.revisions && page.revisions[0];
			if (!revision) {
				throw new Error('无法读取页面源码：' + title);
			}

			return {
				revid: revision.revid,
				wikitext: getRevisionContent(revision)
			};
		});
	}

	function patchPage(api, title, wikitext, baseRevisionId) {
		var params = buildPatchEdit(title, wikitext, baseRevisionId);
		if (!params) {
			return null;
		}

		return api.postWithToken('csrf', params);
	}

	function createPageFromCurrentRevision(api, title, wikitext) {
		var wrapped = wrapHeaderTitleValue(wikitext);

		return api.postWithToken('csrf', {
			action: 'edit',
			title: title,
			text: wrapped.wikitext,
			summary: CREATE_PAGE_SUMMARY,
			createonly: 1,
			formatversion: 2
		});
	}

	function moveAndPatch(button, sourcePageTitle, newTitle, idleTitle) {
		var api = new mw.Api();

		button.disabled = true;
		button.textContent = '正在移動...';

		api.postWithToken('csrf', {
			action: 'move',
			from: sourcePageTitle,
			to: newTitle,
			reason: '',
			formatversion: 2
		}).then(function () {
			button.textContent = '正在更新...';
			return fetchPageWikitext(api, newTitle);
		}).then(function (pageData) {
			return patchPage(api, newTitle, pageData.wikitext, pageData.revid);
		}).then(reloadAfterSuccess).catch(function (error) {
			button.disabled = false;
			button.textContent = idleTitle;
			showError(error);
		});
	}

	function copyCurrentRevision(button, newTitle, idleTitle) {
		var api = new mw.Api();
		var viewedRevisionId = getViewedRevisionId();

		button.disabled = true;
		button.textContent = '正在读取...';

		Promise.resolve().then(function () {
			return fetchRevisionWikitext(api, viewedRevisionId);
		}).then(function (pageData) {
			button.textContent = '正在创建...';
			return createPageFromCurrentRevision(api, newTitle, pageData.wikitext);
		}).then(function () {
			goToPage(newTitle);
		}).catch(function (error) {
			button.disabled = false;
			button.textContent = idleTitle;
			showError(error);
		});
	}

	function addButton() {
		var parserOutput = document.querySelector('.mw-parser-output');
		var marker = parserOutput ? parserOutput.querySelector('#' + CASE_NUMBER_ELEMENT_ID) : null;
		var sourcePageTitle = getCurrentPageTitle();
		var newTitle = marker ? normalizeTitle(marker.getAttribute('data-case-number-title')) : '';
		var isLatestRevision = isViewingLatestRevision();
		var buttonTitle = isLatestRevision ? BUTTON_TITLE : COPY_BUTTON_TITLE;

		if (!marker || !newTitle || sourcePageTitle === newTitle || document.getElementById(BUTTON_ID)) {
			return;
		}

		var button = document.createElement('button');
		button.id = BUTTON_ID;
		button.type = 'button';
		button.className = 'cdx-button';
		button.title = buttonTitle;
		button.textContent = buttonTitle;
		button.addEventListener('click', function () {
			if (isLatestRevision) {
				moveAndPatch(button, sourcePageTitle, newTitle, buttonTitle);
				return;
			}

			copyCurrentRevision(button, newTitle, buttonTitle);
		});

		marker.appendChild(button);
	}

	mw.loader.using('mediawiki.api').then(function () {
		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', addButton);
			return;
		}

		addButton();
	});
}());
// </nowiki>
