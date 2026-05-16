// <nowiki>
(function () {
	'use strict';

	var BUTTON_TITLE = '移動到文書號標題';
	var BUTTON_ID = 'move-to-case-number-title';
	var CASE_NUMBER_ELEMENT_ID = 'prc-court-document-case-number';

	function normalizeTitle(title) {
		return String(title || '').trim().replace(/_/g, ' ');
	}

	function getCurrentPageTitle() {
		return normalizeTitle(mw.config.get('wgRelevantPageName') || mw.config.get('wgPageName'));
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

	function isInsideWikiLink(wikitext, index) {
		var lastOpen = wikitext.lastIndexOf('[[', index);
		var lastClose = wikitext.lastIndexOf(']]', index);
		return lastOpen !== -1 && lastOpen > lastClose;
	}

	function wrapFirstUnlinkedOldTitle(wikitext, oldTitle) {
		var index = wikitext.indexOf(oldTitle);
		var found = false;

		while (index !== -1) {
			found = true;
			if (!isInsideWikiLink(wikitext, index)) {
				return {
					changed: true,
					wikitext: wikitext.slice(0, index) + '[[' + oldTitle + ']]' +
						wikitext.slice(index + oldTitle.length)
				};
			}

			index = wikitext.indexOf(oldTitle, index + oldTitle.length);
		}

		if (!found) {
			throw new Error('在新页面源码中找不到旧标题：' + oldTitle);
		}

		return {
			changed: false,
			wikitext: wikitext
		};
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

	function moveAndPatch(button, oldTitle, newTitle) {
		var api = new mw.Api();

		button.disabled = true;
		button.textContent = '正在移動...';

		api.postWithToken('csrf', {
			action: 'move',
			from: oldTitle,
			to: newTitle,
			reason: '',
			formatversion: 2
		}).then(function () {
			button.textContent = '正在更新...';
			return fetchPageWikitext(api, newTitle);
		}).then(function (pageData) {
			var wrapped = wrapFirstUnlinkedOldTitle(pageData.wikitext, oldTitle);
			if (!wrapped.changed) {
				return null;
			}

			return api.postWithToken('csrf', {
				action: 'edit',
				title: newTitle,
				text: wrapped.wikitext,
				summary: '',
				baserevid: pageData.revid,
				nocreate: 1,
				formatversion: 2
			});
		}).then(reloadAfterSuccess).catch(function (error) {
			button.disabled = false;
			button.textContent = BUTTON_TITLE;
			showError(error);
		});
	}

	function addButton() {
		var parserOutput = document.querySelector('.mw-parser-output');
		var marker = parserOutput ? parserOutput.querySelector('#' + CASE_NUMBER_ELEMENT_ID) : null;
		var oldTitle = getCurrentPageTitle();
		var newTitle = marker ? normalizeTitle(marker.getAttribute('data-case-number-title')) : '';

		if (!marker || !newTitle || oldTitle === newTitle || document.getElementById(BUTTON_ID)) {
			return;
		}

		var button = document.createElement('button');
		button.id = BUTTON_ID;
		button.type = 'button';
		button.className = 'cdx-button';
		button.title = BUTTON_TITLE;
		button.textContent = BUTTON_TITLE;
		button.addEventListener('click', function () {
			moveAndPatch(button, oldTitle, newTitle);
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
